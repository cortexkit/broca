//! Build-task #1b gate: the provider's frozen render config is authoritative on
//! resume via blob-equality drift detection.
//!
//! Render is a pure function of `(FrozenRenderConfig, CallOptions)`, so if the
//! provider supplied on resume serializes the SAME frozen config as the one
//! recorded at run start, it renders byte-identical bytes. The loop verifies that
//! by comparing the opaque blobs (it never deserializes them). This gate proves:
//!  1. a DIVERGENT frozen config on resume → fail-to-doctor (Interrupted);
//!  2. a MATCHING frozen config on resume → the run re-renders BYTE-IDENTICAL bytes;
//!  3. the GENERIC-RESUME path: read the prior config from the WAL, deserialize the
//!     opaque blob to REBUILD the provider, resume → blob-equality passes →
//!     byte-identical. This is the "resume any session" flow a tool would use.

// A file-level `cfg` DELETES this file when the feature is off, and a deleted file reports
// "running 0 tests ... test result: ok" -- indistinguishable at a glance from a suite that
// ran and passed, and impossible to instrument because no code survives to carry a notice.
// Gating a module instead leaves this shell compiled, so the absence can announce itself.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP frozen_config_resume: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-provider/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use broca_core::{
        provider::Provider, read_prior_run_config, run_loop, AllowAll, FileLeaseStore,
        FileWalStore, RunError, RunRequest, RunSummary, StubToolPlane, WalStore,
    };
    use broca_protocol::{
        caller::RunFinishReason,
        content::{AssistantMessage, ContentBlock, Message, Role},
        provider::{NormalizedEvent, ProviderControl},
        request::{CallOptions, ModelHandle, StopCondition, Tool, ToolChoice},
        BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, StepId, Usage,
    };
    use broca_provider::families::{
        anthropic_apikey_frozen_config, anthropic_frozen_config, fresh_admission_config_error,
        openai_chat_frozen_config, AnthropicMessages,
    };
    use broca_provider::policy::ReasoningPolicy;
    use broca_provider::wire::{
        CacheClass, CachePolicy, CacheTtl, FrozenRenderConfig, MarkerSyntax, WireFamily,
    };
    use broca_wal::{FileWal, RunConfig, RunOrigin, Wal, WalRecord};
    use tokio::sync::mpsc;

    /// A provider built on a real `FrozenRenderConfig`: renders via the Anthropic family
    /// and exposes the frozen config as its blob. Records the bytes it renders per step.
    #[derive(Clone)]
    struct FrozenProvider {
        frozen: FrozenRenderConfig,
        render_config_blob: Option<serde_json::Value>,
        sent: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    }

    impl FrozenProvider {
        fn new(frozen: FrozenRenderConfig) -> Self {
            Self {
                frozen,
                render_config_blob: None,
                sent: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn with_render_config_blob(mut self, render_config_blob: serde_json::Value) -> Self {
            self.render_config_blob = Some(render_config_blob);
            self
        }
    }

    #[async_trait]
    impl Provider for FrozenProvider {
        async fn step(
            &self,
            step_id: StepId,
            opts: &CallOptions,
        ) -> mpsc::Receiver<NormalizedEvent> {
            let bytes = self.render_request(opts);
            self.sent
                .lock()
                .unwrap()
                .insert((step_id.0 - 1) as usize, bytes);

            let (tx, rx) = mpsc::channel(16);
            tokio::spawn(async move {
                let _ = tx
                    .send(NormalizedEvent::Control(
                        ProviderControl::AssistantMessage {
                            message: AssistantMessage {
                                message_id: MessageId("m1".into()),
                                content: vec![ContentBlock::Text { text: "hi".into() }],
                            },
                        },
                    ))
                    .await;
                let _ = tx
                    .send(NormalizedEvent::Control(ProviderControl::Finished {
                        usage: Usage::default(),
                        finish_reason: FinishReason::Stop,
                        retries_used: 0,
                    }))
                    .await;
            });
            rx
        }

        fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
            AnthropicMessages.render(&self.frozen, opts)
        }

        fn renderer_version(&self) -> u32 {
            self.frozen.renderer_version
        }

        fn render_config_blob(&self) -> serde_json::Value {
            self.render_config_blob.clone().unwrap_or_else(|| {
                serde_json::to_value(&self.frozen).expect("frozen render config must serialize")
            })
        }

        fn fresh_admission_config_error(
            &self,
            generation: &GenerationConfig,
            tool_choice: &ToolChoice,
        ) -> Option<String> {
            fresh_admission_config_error(&self.frozen, generation, tool_choice)
        }
    }

    fn request(dir: &std::path::Path) -> RunRequest {
        RunRequest {
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("r".into()),
            session: BindIdentity::new(dir, "c6b", "s1"),
            model: ModelHandle::new("anthropic", "claude-opus-4-8"),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text { text: "hi".into() }],
            }],
            tools: vec![],
            policy_allowed: Default::default(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![StopCondition::StepCount { n: 5 }],
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
        }
    }

    fn request_with_tool_choice(dir: &std::path::Path, tool_choice: ToolChoice) -> RunRequest {
        let mut request = request(dir);
        request.tools.push(Tool {
            name: "weather".into(),
            description: "Read the weather".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: None,
            execution_mode: Default::default(),
        });
        request.policy_allowed.insert("weather".into());
        request.tool_choice = tool_choice;
        request
    }

    async fn drive_request(
        dir: &std::path::Path,
        provider: &FrozenProvider,
        request: RunRequest,
    ) -> Result<RunSummary, RunError> {
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let result = run_loop(
            request,
            provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            &tx,
        )
        .await;
        drop(tx);
        collector.await.unwrap();
        result
    }

    async fn run_once(dir: &std::path::Path, provider: &FrozenProvider) -> RunFinishReason {
        drive_request(dir, provider, request(dir))
            .await
            .expect("run ok")
            .reason
    }

    fn wal_path(dir: &std::path::Path) -> std::path::PathBuf {
        FileWalStore::new(dir.join("wal")).wal_path(&request(dir).session)
    }

    /// Truncate the WAL to its first `keep` records (simulate a crash right after them).
    fn truncate_wal(dir: &std::path::Path, keep: usize) {
        let path = wal_path(dir);
        let framed = FileWal::open(&path).unwrap().replay().unwrap();
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for f in framed.into_iter().take(keep) {
            wal.append(f.fence, &f.record, true).unwrap();
        }
    }

    /// Drive a fresh run to completion, then truncate to ONLY the RunStarted record so a
    /// resume must re-run step 1 (and hit the drift check). Returns the reference step-0
    /// bytes for byte-identity comparison.
    async fn seed_crashed_run(dir: &std::path::Path, frozen: &FrozenRenderConfig) -> Vec<u8> {
        let provider = FrozenProvider::new(frozen.clone());
        let reason = run_once(dir, &provider).await;
        assert_eq!(reason, RunFinishReason::Completed);
        let ref_bytes = provider
            .sent
            .lock()
            .unwrap()
            .get(&0)
            .cloned()
            .expect("step 0 sent");
        // Keep only RunStarted (seq 1) → resume re-runs step 1.
        truncate_wal(dir, 1);
        ref_bytes
    }

    fn seed_resumable_request(
        dir: &std::path::Path,
        request: &RunRequest,
        frozen: &FrozenRenderConfig,
    ) {
        let wal_store = FileWalStore::new(dir.join("wal"));
        let mut wal = wal_store
            .open(&request.session)
            .expect("open WAL for resumable seed");
        let config = RunConfig {
            model: request.model.clone(),
            tools: request.tools.clone().into(),
            tool_choice: request.tool_choice.clone(),
            generation: request.generation.clone(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: frozen.renderer_version,
            frozen_render_config: serde_json::to_value(frozen)
                .expect("frozen render config must serialize"),
            auth_selection: None,
            tools_were_omitted: false,
            budget: request.budget.clone(),
            context_limit: request.context_limit,
            work_class: request.work_class.clone(),
            variant: request.variant.clone(),
            service_tier: None,
            stop_when: request.stop_when.clone(),
            context_management: None,
        };
        wal.append(
            1,
            &WalRecord::RunStarted {
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: request.run_id.clone(),
                session: request.session.clone(),
                config: Box::new(config),
                input: request.input.clone().into(),
                origin: RunOrigin::Fresh,
                submission_id: request.submission_id.clone(),
            },
            true,
        )
        .expect("seed previously admitted RunStarted");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_oauth_interleaved_thinking_refuses_non_one_temperature_at_fresh_admission() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-anthropic-temp-fresh-refusal-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut request = request(&dir);
        request.generation.temperature = Some(0.0);
        let session = request.session.clone();
        let provider = FrozenProvider::new(anthropic_frozen_config("claude-opus-4-8"));

        let error = drive_request(&dir, &provider, request)
            .await
            .expect_err("the OAuth thinking conflict must fail fresh admission");

        match error {
            RunError::Config(message) => assert_eq!(
                message,
                "generation.temperature=0 conflicts with reasoning.mode='anthropic_interleaved': \
                 the caller set generation.temperature, while the resolved Anthropic OAuth auth \
                 method enabled thinking; Anthropic requires generation.temperature=1 when \
                 thinking is enabled"
            ),
            other => panic!("expected typed Config error, got {other:?}"),
        }
        assert!(
            provider.sent.lock().unwrap().is_empty(),
            "a refused admission must not reach the provider"
        );
        assert!(
            FileWalStore::new(dir.join("wal"))
                .read_records(&session)
                .expect("read refused run WAL")
                .is_empty(),
            "a refused fresh admission must not write RunStarted"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_oauth_interleaved_thinking_accepts_temperature_one_at_fresh_admission() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-anthropic-temp-one-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut request = request(&dir);
        request.generation.temperature = Some(1.0);
        let provider = FrozenProvider::new(anthropic_frozen_config("claude-opus-4-8"));

        let summary = drive_request(&dir, &provider, request)
            .await
            .expect("temperature one is valid with Anthropic thinking");

        assert_eq!(summary.reason, RunFinishReason::Completed);
        assert_eq!(provider.sent.lock().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_api_key_temperature_zero_remains_admissible() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-anthropic-apikey-temp-zero-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut request = request(&dir);
        request.generation.temperature = Some(0.0);
        let frozen = anthropic_apikey_frozen_config("claude-opus-4-8");
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);
        let provider = FrozenProvider::new(frozen);

        let summary = drive_request(&dir, &provider, request)
            .await
            .expect("API-key resolution disables interleaved thinking");

        assert_eq!(summary.reason, RunFinishReason::Completed);
        let sent = provider.sent.lock().unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(sent.get(&0).expect("the admitted API-key request rendered"))
                .expect("rendered body is JSON");
        assert!(body.get("thinking").is_none());
        assert_eq!(body["temperature"], serde_json::json!(0.0));
        drop(sent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_anthropic_interleaved_temperature_conflict_remains_resumable() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-anthropic-temp-legacy-resume-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut request = request(&dir);
        request.generation.temperature = Some(0.0);
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        seed_resumable_request(&dir, &request, &frozen);
        let provider = FrozenProvider::new(frozen);

        let summary = drive_request(&dir, &provider, request)
            .await
            .expect("a previously admitted conflicting config must remain resumable");

        assert_eq!(summary.reason, RunFinishReason::Completed);
        let sent = provider.sent.lock().unwrap();
        let body: serde_json::Value = serde_json::from_slice(
            sent.get(&0)
                .expect("resume reached its previously admitted provider"),
        )
        .expect("rendered body is JSON");
        assert!(body.get("thinking").is_some());
        assert_eq!(body["temperature"], serde_json::json!(0.0));
        drop(sent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_oauth_interleaved_thinking_refuses_forced_tool_choices_at_fresh_admission() {
        let cases = [
            ("required", ToolChoice::Required, r#"{"type":"required"}"#),
            (
                "tool",
                ToolChoice::Tool {
                    name: "weather".into(),
                },
                r#"{"type":"tool","name":"weather"}"#,
            ),
        ];

        for (case, tool_choice, config_value) in cases {
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-anthropic-thinking-forced-{case}-fresh-refusal-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let request = request_with_tool_choice(&dir, tool_choice);
            let session = request.session.clone();
            let provider = FrozenProvider::new(anthropic_frozen_config("claude-opus-4-8"));

            let error = drive_request(&dir, &provider, request)
                .await
                .expect_err("manual thinking plus forced tool use must fail fresh admission");

            match error {
                RunError::Config(message) => assert_eq!(
                    message,
                    format!(
                        "tool_choice={config_value} conflicts with \
                         reasoning.mode='anthropic_interleaved': the caller set tool_choice, while \
                         the resolved Anthropic OAuth auth method enabled thinking; Anthropic \
                         rejects this combination: \"Thinking may not be enabled when tool_choice \
                         forces tool use.\""
                    )
                ),
                other => panic!("expected typed Config error, got {other:?}"),
            }
            assert!(
                provider.sent.lock().unwrap().is_empty(),
                "a refused admission must not reach the provider"
            );
            assert!(
                FileWalStore::new(dir.join("wal"))
                    .read_records(&session)
                    .expect("read refused run WAL")
                    .is_empty(),
                "a refused fresh admission must not write RunStarted"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_oauth_interleaved_thinking_accepts_non_forcing_tool_choices() {
        let cases = [
            (
                "auto",
                ToolChoice::Auto,
                serde_json::json!({"type": "auto"}),
            ),
            (
                "none",
                ToolChoice::None,
                serde_json::json!({"type": "none"}),
            ),
        ];

        for (case, tool_choice, expected_wire_choice) in cases {
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-anthropic-thinking-{case}-admitted-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let request = request_with_tool_choice(&dir, tool_choice);
            let provider = FrozenProvider::new(anthropic_frozen_config("claude-opus-4-8"));

            let summary = drive_request(&dir, &provider, request)
                .await
                .expect("non-forcing tool choices are valid with manual thinking");

            assert_eq!(summary.reason, RunFinishReason::Completed);
            let sent = provider.sent.lock().unwrap();
            let body: serde_json::Value = serde_json::from_slice(
                sent.get(&0)
                    .expect("the admitted non-forcing request reached the provider"),
            )
            .expect("rendered body is JSON");
            assert_eq!(body["thinking"]["type"], serde_json::json!("enabled"));
            assert_eq!(body["tool_choice"], expected_wire_choice);
            drop(sent);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_api_key_forced_tool_choices_remain_admissible() {
        let cases = [
            (
                "required",
                ToolChoice::Required,
                serde_json::json!({"type": "any"}),
            ),
            (
                "tool",
                ToolChoice::Tool {
                    name: "weather".into(),
                },
                serde_json::json!({"type": "tool", "name": "weather"}),
            ),
        ];

        for (case, tool_choice, expected_wire_choice) in cases {
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-anthropic-apikey-forced-{case}-admitted-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let request = request_with_tool_choice(&dir, tool_choice);
            let frozen = anthropic_apikey_frozen_config("claude-opus-4-8");
            assert_eq!(frozen.reasoning, ReasoningPolicy::None);
            let provider = FrozenProvider::new(frozen);

            let summary = drive_request(&dir, &provider, request)
                .await
                .expect("forced tool choices remain valid without manual thinking");

            assert_eq!(summary.reason, RunFinishReason::Completed);
            let sent = provider.sent.lock().unwrap();
            let body: serde_json::Value = serde_json::from_slice(
                sent.get(&0)
                    .expect("the admitted API-key request reached the provider"),
            )
            .expect("rendered body is JSON");
            assert!(body.get("thinking").is_none());
            assert_eq!(body["tool_choice"], expected_wire_choice);
            drop(sent);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_anthropic_interleaved_forced_tool_choice_conflicts_remain_resumable() {
        let cases = [
            (
                "required",
                ToolChoice::Required,
                serde_json::json!({"type": "any"}),
            ),
            (
                "tool",
                ToolChoice::Tool {
                    name: "weather".into(),
                },
                serde_json::json!({"type": "tool", "name": "weather"}),
            ),
        ];

        for (case, tool_choice, expected_wire_choice) in cases {
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-anthropic-forced-{case}-legacy-resume-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let request = request_with_tool_choice(&dir, tool_choice);
            let frozen = anthropic_frozen_config("claude-opus-4-8");
            seed_resumable_request(&dir, &request, &frozen);
            let provider = FrozenProvider::new(frozen);

            let summary = drive_request(&dir, &provider, request)
                .await
                .expect("a previously admitted forced-tool conflict must remain resumable");

            assert_eq!(summary.reason, RunFinishReason::Completed);
            let sent = provider.sent.lock().unwrap();
            let body: serde_json::Value = serde_json::from_slice(
                sent.get(&0)
                    .expect("resume reached its previously admitted provider"),
            )
            .expect("rendered body is JSON");
            assert_eq!(body["thinking"]["type"], serde_json::json!("enabled"));
            assert_eq!(body["tool_choice"], expected_wire_choice);
            drop(sent);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn divergent_frozen_config_on_resume_fails_to_doctor() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-divergent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Seed a crashed run frozen with the Anthropic config.
        let original = anthropic_frozen_config("claude-opus-4-8");
        let _ref_bytes = seed_crashed_run(&dir, &original).await;

        // Resume with a DIVERGENT frozen config (a different family entirely — its blob
        // cannot equal the recorded Anthropic blob).
        let divergent = openai_chat_frozen_config("gpt-4o");
        let provider = FrozenProvider::new(divergent);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop(
            request(&dir),
            &provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .expect("run ok");
        drop(tx);
        collector.await.unwrap();

        // The drift is caught → fail-to-doctor, and the provider was NEVER asked to
        // render (no divergent bytes ever sent).
        assert_eq!(
            summary.reason,
            RunFinishReason::Interrupted,
            "divergent frozen config must fail-to-doctor"
        );
        assert!(
            provider.sent.lock().unwrap().is_empty(),
            "a divergent-config resume must not render any step"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_frozen_config_on_resume_fails_with_a_clear_error() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-null-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let frozen = anthropic_frozen_config("claude-opus-4-8");
        let seed =
            FrozenProvider::new(frozen.clone()).with_render_config_blob(serde_json::Value::Null);
        assert_eq!(run_once(&dir, &seed).await, RunFinishReason::Completed);
        truncate_wal(&dir, 1);

        let provider = FrozenProvider::new(frozen).with_render_config_blob(serde_json::Value::Null);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = rx.recv().await {
                events.push(event);
            }
            events
        });
        let summary = run_loop(
            request(&dir),
            &provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .expect("run ok");
        drop(tx);
        let events = collector.await.unwrap();

        assert_eq!(summary.reason, RunFinishReason::Interrupted);
        assert!(
            provider.sent.lock().unwrap().is_empty(),
            "a null-config resume must stop before rendering"
        );
        let error = events
            .iter()
            .find_map(|event| match event {
                broca_core::OutputEvent::Control(broca_protocol::ControlUnit::Error { error }) => {
                    Some(error)
                }
                _ => None,
            })
            .expect("null render config should produce a clear error");
        assert_eq!(
            error.provider_code.as_deref(),
            Some("frozen_render_config_unverifiable")
        );
        assert!(error.message.contains("old WALs"));
        assert!(error.message.contains("must be discarded"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn matching_frozen_config_on_resume_is_byte_identical() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-matching-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let original = anthropic_frozen_config("claude-opus-4-8");
        let ref_bytes = seed_crashed_run(&dir, &original).await;

        // Resume with a freshly-built but EQUAL frozen config.
        let provider = FrozenProvider::new(anthropic_frozen_config("claude-opus-4-8"));
        let reason = run_once(&dir, &provider).await;

        assert_eq!(
            reason,
            RunFinishReason::Completed,
            "matching config must resume + complete"
        );
        let resumed_bytes = provider
            .sent
            .lock()
            .unwrap()
            .get(&0)
            .cloned()
            .expect("resume rendered step 0");
        assert_eq!(
            resumed_bytes, ref_bytes,
            "matching frozen config must re-render byte-identical bytes"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn generic_resume_rebuilds_provider_from_blob_and_is_byte_identical() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-generic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let original = anthropic_frozen_config("claude-opus-4-8");
        let ref_bytes = seed_crashed_run(&dir, &original).await;

        // GENERIC RESUME: a tool that doesn't know the original provider reads the prior
        // run_config from the WAL, deserializes the OPAQUE blob into a FrozenRenderConfig,
        // and rebuilds the matching provider — all without being told what it was.
        let wal = FileWalStore::new(dir.join("wal"));
        let prior = read_prior_run_config(&wal, &request(&dir).session)
            .expect("read prior config")
            .expect("a run was started");
        assert!(
            !prior.frozen_render_config.is_null(),
            "the frozen render config blob must be persisted"
        );

        let rebuilt: FrozenRenderConfig =
            serde_json::from_value(prior.frozen_render_config.clone())
                .expect("blob deserializes to FrozenRenderConfig");
        // The rebuilt config equals the original (lossless round-trip).
        assert_eq!(rebuilt, original, "the blob must round-trip losslessly");

        let provider = FrozenProvider::new(rebuilt);
        let reason = run_once(&dir, &provider).await;

        assert_eq!(
            reason,
            RunFinishReason::Completed,
            "rebuilt-from-blob provider must resume + complete"
        );
        let resumed_bytes = provider
            .sent
            .lock()
            .unwrap()
            .get(&0)
            .cloned()
            .expect("resume rendered step 0");
        assert_eq!(
            resumed_bytes, ref_bytes,
            "a provider rebuilt from the persisted blob must re-render byte-identical bytes"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// First-party Anthropic's no-request default, exactly as `resolve_cache_policy` in
    /// broca-module-serve freezes it (that crate's tests pin the same value).
    #[tokio::test]
    async fn legacy_session_retains_marker_prefix_on_new_episode_and_resume() {
        let dir = broca_test_support::scratch_root()
            .join(format!("legacy-marker-prefix-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        let mut policy = default_anthropic_cache_policy();
        if let CacheClass::Markers {
            max_breakpoints, ..
        } = &mut policy.class
        {
            *max_breakpoints = 2;
        }
        frozen.cache = Some(policy);
        let mut req = request(&dir);
        req.input = vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "system".into(),
                }],
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: "m0".into() }]),
            Message::new(
                Role::Assistant,
                vec![ContentBlock::Text { text: "m1".into() }],
            ),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "tail".into(),
                }],
            ),
        ];
        seed_resumable_request(&dir, &req, &frozen);
        let reference = AnthropicMessages.render(&frozen, &CallOptions::new(req.input.clone()));
        let resumed = FrozenProvider::new(frozen.clone());
        drive_request(&dir, &resumed, req.clone()).await.unwrap();
        assert_eq!(resumed.sent.lock().unwrap()[&0], reference);
        let mut appended_req = request(&dir);
        appended_req.run_id = RunId("r2".into());
        let appended = FrozenProvider::new(frozen);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        broca_core::run_loop_append_episode(
            appended_req,
            &appended,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            None,
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        collector.await.unwrap();
        let original: serde_json::Value = serde_json::from_slice(&reference).unwrap();
        let next: serde_json::Value =
            serde_json::from_slice(&appended.sent.lock().unwrap()[&0]).unwrap();
        assert_eq!(
            broca_protocol::canonical_json_bytes(&original["system"]),
            broca_protocol::canonical_json_bytes(&next["system"])
        );
        let prefix_len = original["messages"].as_array().unwrap().len();
        assert_eq!(
            broca_protocol::canonical_json_bytes(&original["messages"]),
            broca_protocol::canonical_json_bytes(&serde_json::Value::Array(
                next["messages"].as_array().unwrap()[..prefix_len].to_vec()
            ))
        );
        let versions: Vec<_> = wal
            .read_records(&req.session)
            .unwrap()
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted {
                    marker_order_version,
                    ..
                } => Some(*marker_order_version),
                _ => None,
            })
            .collect();
        assert_eq!(versions, vec![None, None]);
    }

    fn default_anthropic_cache_policy() -> CachePolicy {
        CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ttl: CacheTtl::FiveMinutes,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: true,
            },
        }
    }

    /// Resume the seeded run the way the executor does: read the recorded blob, take its
    /// frozen cache policy as-is (never re-resolving it from the resuming send), and rebuild.
    async fn resume_with_the_recorded_cache_policy(dir: &std::path::Path) -> Vec<u8> {
        let wal = FileWalStore::new(dir.join("wal"));
        let prior = read_prior_run_config(&wal, &request(dir).session)
            .expect("read prior config")
            .expect("a run was started");
        let recorded: FrozenRenderConfig =
            serde_json::from_value(prior.frozen_render_config).expect("blob deserializes");
        let mut rebuilt = anthropic_frozen_config("claude-opus-4-8");
        rebuilt.cache = recorded.cache;
        let provider = FrozenProvider::new(rebuilt);
        assert_eq!(run_once(dir, &provider).await, RunFinishReason::Completed);
        let bytes = provider
            .sent
            .lock()
            .unwrap()
            .get(&0)
            .cloned()
            .expect("resume rendered step 0");
        bytes
    }

    /// A run admitted before first-party Anthropic cached by default froze no policy. It must
    /// resume with no markers, byte for byte -- and a resume that re-resolved the policy from
    /// today's default instead would not render different bytes, it would be refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_run_admitted_without_a_cache_policy_resumes_without_markers() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-legacy-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let admitted = anthropic_frozen_config("claude-opus-4-8");
        assert_eq!(admitted.cache, None);
        let ref_bytes = seed_crashed_run(&dir, &admitted).await;

        let resumed = resume_with_the_recorded_cache_policy(&dir).await;
        assert_eq!(resumed, ref_bytes, "byte-identical resume");
        assert!(!String::from_utf8_lossy(&resumed).contains("cache_control"));

        // The counterfactual: re-resolving would freeze the default into the resumed config.
        truncate_wal(&dir, 1);
        let mut re_resolved = anthropic_frozen_config("claude-opus-4-8");
        re_resolved.cache = Some(default_anthropic_cache_policy());
        let provider = FrozenProvider::new(re_resolved);
        assert_eq!(
            run_once(&dir, &provider).await,
            RunFinishReason::Interrupted,
            "a re-resolved cache policy is config drift and must stop the resume"
        );
        assert!(provider.sent.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A run admitted with the default froze it, and resumes rendering the same markers.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_run_admitted_with_the_default_cache_policy_resumes_with_it() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-c6b-default-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut admitted = anthropic_frozen_config("claude-opus-4-8");
        admitted.cache = Some(default_anthropic_cache_policy());
        let ref_bytes = seed_crashed_run(&dir, &admitted).await;
        let text = String::from_utf8_lossy(&ref_bytes).into_owned();
        assert!(
            text.contains(r#""cache_control":{"type":"ephemeral"}"#),
            "the default marker, with no ttl, is on the admitted request: {text}"
        );

        let resumed = resume_with_the_recorded_cache_policy(&dir).await;
        assert_eq!(resumed, ref_bytes, "byte-identical resume");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
