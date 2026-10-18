//! The broca subc MODULE serve adapter.
//!
//! HELLO-registers broca as a subc ManagementSurface, then dispatches inbound
//! `session.*`/`run.*` route requests to the `broca_module` coordinator
//! (`ServeHandler`), streaming `session.subscribe` as `StreamData` frames. This is the
//! thin WIRE adapter — the dispatch logic is `broca_module` (transport-agnostic). The
//! pattern mirrors the other Rust subc modules' hand-rolled serve binaries
//! (ai-provider-quota's quota-module is the template).
#![forbid(unsafe_code)]

/// Production `role.describe` and the conformance subject share this retention writer switch.
/// The released v0.3.193 daemon reads retained WALs and deletes expired content, so reverting
/// to that reader after this daemon writes retained sessions preserves deletion promises.
pub const RETENTION_WRITER_ENABLED: bool = true;

pub mod account_routing;
pub mod agent_data;
pub mod auth_routing;
pub mod billing_routing;
pub mod bus_events;
pub mod cache_dump;
pub mod cache_routing;
pub mod cache_warm;
pub mod catalog_refresh;
pub mod claude_code_identity;
pub mod client;
pub mod codeassist_identity;
pub mod config_home;
pub mod consumer_routes;
pub mod context_management;
pub mod keep_warm_quota;
pub mod manifest;
pub mod provider_overrides;
pub mod run_routing;
pub mod send_audit;
pub mod serve;
pub mod session_class;
pub mod stall;
pub mod state_root;
pub mod transport_config;
pub mod vault_audit;
pub mod vault_config;
pub mod wal_archive;
pub mod wal_render;
pub mod wire;

pub use auth_routing::{family_for_method, AuthMethod, CredentialId, MethodConfig};
pub use billing_routing::{ChargeBasisTable, CHARGE_BASIS_MAPPING_VERSION};
pub use cache_routing::{
    resolve_cache_policy, resolve_caller_cache_key, resolve_prompt_cache_key, CacheTtlTable,
};
pub use catalog_refresh::{spawn_catalog_poll, CatalogPoller, CATALOG_POLL_INTERVAL};
pub use client::SessionClient;
pub use consumer_routes::ConsumerRoutes;
pub use run_routing::start_mode_after_auth;
pub use serve::{
    serve_connection, serve_connection_configured, serve_connection_with_catalog, ServeEnd,
    ServeIoError,
};
pub use vault_config::{
    scoped_not_found_message, VaultAddressing, VaultCredentialIds, VaultHandles, VaultHandlesError,
};
