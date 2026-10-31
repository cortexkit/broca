//! subc adapters for the run loop's tool, compaction, and hook planes.
//!
//! [`SubcToolPlane`] implements [`broca_core::tool_plane::ToolPlane`], dispatching tool calls
//! through subc to tool providers over an authenticated, multiplexed consumer
//! connection ([`SubcConnection`]). [`SubcCompactionPlane`] and [`SubcHookPlane`]
//! adapt compaction and step-transform providers to the corresponding core traits.
//! [`SubcPlanFetcher`] and [`SubcPlanAdmitter`] fetch and admit session plans.
//! The core consumes these broca-facing traits rather than daemon socket types;
//! this crate maps identities and provider messages at the adapter edge.
//!
//! ## Dependency sources
//!
//! The subc protocol, transport, control, and client crates inherit exact registry
//! pins from the root `Cargo.toml`'s `workspace.dependencies`; `Cargo.lock` records
//! the resolved graph. Linked dependencies do not come from sibling checkouts;
//! those checkouts are used only to build integration-test rig binaries.
#![forbid(unsafe_code)]

#[cfg(all(feature = "test-clock", not(debug_assertions)))]
compile_error!("broca-subc's test-clock feature must not be enabled in a release build");

pub mod compaction_plane;
pub mod connection;
pub mod consent;
pub mod consent_card;
pub mod content_v1;
pub mod flow_providers;
pub mod hook_plane;
pub mod late_results;
pub mod plan_fetch;
pub mod private_content;
mod role_route;
mod route_close;
pub mod scope_describe;
pub mod session_door;
pub mod subc_plane;
pub mod tcp_connect;
pub mod tool_deadline;
pub mod vault;

pub use compaction_plane::SubcCompactionPlane;
pub use connection::{
    classifier_role_versions, rejection_is_unknown_channel, ConnError, FrameDropGauge,
    ModuleHealthSnapshot, RouteHandle, RouteOpenRetry, SubcConnection, SubscribeStream,
    CLASSIFIER_ROLE_VERSIONS, FRAME_DROPS, ROUTE_OPEN_ATTEMPT_TIMEOUT, ROUTE_OPEN_RETRY_DEADLINE,
    ROUTE_OPEN_RETRY_INTERVAL,
};
pub use consent::{
    AnswersPage, ConsentClient, ConsentError, ConsentRecord, ConsentState, FilingReply,
    ScopedConsentPlane, WithdrawalReply,
};
pub use flow_providers::{
    ApprovedFlowProviders, FlowPolicy, FlowProvidersClient, FlowProvidersError,
};
pub use hook_plane::SubcHookPlane;
pub use late_results::{late_entry_tool_result, LateResultsError, LateResultsFeed};
pub use plan_fetch::{SubcPlanAdmitter, SubcPlanFetcher};
pub use private_content::{ImageResolveError, ResolvedImage};
pub use private_content::{PrivateContent, PrivateContentClient, PrivateContentError, SourceKind};
pub use scope_describe::{DescribedScope, ModuleScopeDescriber, ScopeDescriber};
pub use session_door::{DoorCoverage, DoorRead, SessionDoorClient, SessionDoorError};
pub use subc_plane::SubcToolPlane;
pub use vault::{VaultAddress, VaultClient, VaultError, VaultGet, VaultToken};
