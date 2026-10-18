//! `LiveAnthropicProvider` — a convenience constructor for an Anthropic live
//! provider (feature `live`): the Anthropic Messages family + an OAuth token read
//! from disk, wrapped around the generic [`crate::transport::LiveProvider`].
//!
//! All transport is the generic driver's; this module owns only Anthropic auth
//! (reading an already-minted OAuth token from disk — minting/refreshing is out of
//! scope) and the convenience builder API.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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

#[derive(Debug)]
pub enum LiveAuthError {
    Io(std::io::Error),
    Parse(String),
    /// No anthropic OAuth token present.
    Missing,
    /// The token is present but expired. This module does not refresh tokens.
    Expired {
        expires_ms: u64,
        now_ms: u64,
    },
}

impl std::fmt::Display for LiveAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiveAuthError::Io(e) => write!(f, "read auth file: {e}"),
            LiveAuthError::Parse(m) => write!(f, "parse auth file: {m}"),
            LiveAuthError::Missing => write!(f, "no anthropic OAuth token in auth file"),
            LiveAuthError::Expired { expires_ms, now_ms } => write!(
                f,
                "anthropic OAuth token expired (expires={expires_ms} now={now_ms}) — re-authenticate to get a fresh token"
            ),
        }
    }
}

impl std::error::Error for LiveAuthError {}

/// The default OpenCode auth file path.
pub fn default_auth_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    PathBuf::from(home).join(".local/share/opencode/auth.json")
}

/// Read a CURRENTLY-VALID anthropic OAuth access token from disk. Errors if
/// missing or expired — the caller surfaces "need a fresh token"; this function
/// does NOT attempt to refresh the token.
pub fn read_oauth_token(path: &Path) -> Result<String, LiveAuthError> {
    let bytes = std::fs::read(path).map_err(LiveAuthError::Io)?;
    let v: Value =
        serde_json::from_slice(&bytes).map_err(|e| LiveAuthError::Parse(e.to_string()))?;
    let a = v.get("anthropic").ok_or(LiveAuthError::Missing)?;
    let access = a
        .get("access")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(LiveAuthError::Missing)?;
    if let Some(exp) = a.get("expires").and_then(Value::as_u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // Small skew margin so we don't start a call that expires mid-flight.
        if exp <= now + 5_000 {
            return Err(LiveAuthError::Expired {
                expires_ms: exp,
                now_ms: now,
            });
        }
    }
    Ok(access.to_owned())
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

    /// Construct from the default OpenCode auth file. Errors if the token is
    /// missing or expired.
    pub fn from_opencode_auth(model_id: impl Into<String>) -> Result<Self, LiveAuthError> {
        let token = read_oauth_token(&default_auth_path())?;
        Ok(Self::new(model_id, token))
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
