//! `LiveAnthropicProvider` — a convenience builder for an Anthropic live provider
//! (feature `live`), wrapping the generic transport with a caller-supplied token.

use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::Provider;
use broca_protocol::{CallOptions, GenerationConfig, NormalizedEvent, StepId, ToolChoice};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::families::{anthropic_frozen_config, AnthropicMessages};
use crate::transport::LiveProvider;
use crate::wire::AuthHandle;

/// A live Anthropic provider — a thin wrapper over `LiveProvider<AnthropicMessages>`
/// with Anthropic-specific construction (OAuth token, max-tokens builder).
pub struct LiveAnthropicProvider {
    inner: LiveProvider<AnthropicMessages>,
}

impl LiveAnthropicProvider {
    pub fn new(model_id: impl Into<String>, token: impl Into<String>) -> Self {
        let inner = LiveProvider::new(
            AnthropicMessages,
            anthropic_frozen_config(model_id),
            AuthHandle::Bearer(token.into()),
        );
        Self { inner }
    }

    pub fn with_max_tokens(mut self, max: u32) -> Self {
        Arc::get_mut(&mut self.inner.frozen)
            .expect("a newly constructed provider has an exclusive frozen config")
            .max_output_tokens = max;
        self
    }
}

#[async_trait]
impl Provider for LiveAnthropicProvider {
    fn reasoning_is_prefix_bound(&self) -> bool {
        self.inner.reasoning_is_prefix_bound()
    }

    fn structured_output_grammar_rejection(&self, error: &broca_protocol::ProviderError) -> bool {
        self.inner.structured_output_grammar_rejection(error)
    }

    fn downgrade_structured_output(&self) {
        self.inner.downgrade_structured_output();
    }
    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.inner.step_with_runtime(step_id, opts, runtime).await
    }
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.inner.step(step_id, opts).await
    }

    async fn step_with_cancel(
        &self,
        step_id: StepId,
        opts: &CallOptions,
        cancel: CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.inner
            .step_with_cancel(step_id, opts, cancel, deadline)
            .await
    }

    async fn step_with_shared_cancel(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.inner
            .step_with_shared_cancel(step_id, opts, cancel, deadline)
            .await
    }

    // Forwarded like reports_attempt_evidence, which this wrapper already forwards: the trait
    // default IGNORES the seal, so a wrapper that doesn't forward it silently loses the
    // sealed_in_flight record while still claiming to report attempt evidence. Only tests
    // construct this wrapper today; the forward keeps it from diverging if that changes.
    async fn step_with_shutdown_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<tokio::time::Instant>,
        seal: broca_core::ShutdownSeal,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.inner
            .step_with_shutdown_seal(step_id, opts, cancel, deadline, seal)
            .await
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.inner.render_request(opts)
    }

    fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        self.inner.output_limits()
    }

    fn renderer_version(&self) -> u32 {
        self.inner.renderer_version()
    }

    fn reproduces_renderer_version(&self, recorded: u32) -> bool {
        self.inner.reproduces_renderer_version(recorded)
    }

    fn render_config_blob_for_version(&self, renderer_version: u32) -> Value {
        self.inner.render_config_blob_for_version(renderer_version)
    }

    fn render_config_blob(&self) -> Value {
        self.inner.render_config_blob()
    }

    fn fresh_admission_config_error(
        &self,
        generation: &GenerationConfig,
        tool_choice: &ToolChoice,
    ) -> Option<String> {
        self.inner
            .fresh_admission_config_error(generation, tool_choice)
    }

    fn reports_attempt_evidence(&self) -> bool {
        self.inner.reports_attempt_evidence()
    }
}
