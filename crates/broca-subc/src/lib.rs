//! The real subc tool plane. A [`SubcToolPlane`] (2nd [`broca_core::ToolPlane`]
//! impl) dispatches the loop's tool calls runner→subc→AFT over an authenticated,
//! multiplexed consumer connection ([`SubcConnection`]). Zero loop changes — the
//! loop only ever sees the trait.
//!
//! ## Two subc-protocol/transport instances — INTENTIONAL, not a bug
//!
//! The runner binary carries TWO instances each of `subc-protocol` and
//! `subc-transport`:
//!  - **crates.io** (subc-protocol 0.9 / subc-transport 0.4): used by
//!    `broca-protocol`/`broca-provider` for the direct runner↔provider socket's
//!    frame codec.
//!  - **path-from-checkout**: used by THIS crate for the DAEMON socket, because
//!    `subc-control` is unpublished and only available by path, and the adapter
//!    should bind the SAME `subc-protocol` the local daemon it talks to is built
//!    from.
//!
//! This is safe: the two are isolated BY SOCKET and never exchange subc types
//! across the `broca-core`↔`broca-subc` boundary — that boundary is the `ToolPlane`
//! trait, which speaks ONLY broca types (`broca_protocol::BindIdentity`, mapped to
//! `subc_protocol::BindIdentity` in ONE place at this adapter's edge). Two codec
//! copies in the binary is negligible. Do NOT collapse this by path-dep'ing the
//! pure crates to the checkout — that loses their crates.io-clean,
//! standalone-buildable property. When `subc-control` is published, this crate
//! can move to all-crates.io subc deps and the doubling collapses on its own.
#![forbid(unsafe_code)]

pub mod compaction_plane;
pub mod connection;
pub mod elicitation;
pub mod hook_plane;
pub mod plan_fetch;
mod role_route;
pub mod scope_describe;
pub mod session_door;
pub mod subc_plane;
pub mod tool_deadline;
pub mod vault;

pub use compaction_plane::SubcCompactionPlane;
pub use connection::{
    rejection_is_module_reloading, rejection_is_unknown_channel, ConnError, FrameDropGauge,
    ModuleHealthSnapshot, RouteHandle, RouteOpenRetry, SubcConnection, SubscribeStream,
    FRAME_DROPS, ROUTE_OPEN_ATTEMPT_TIMEOUT, ROUTE_OPEN_RETRY_DEADLINE, ROUTE_OPEN_RETRY_INTERVAL,
};
pub use elicitation::{
    AnswersCursor, AnswersPage, ElicitationClient, ElicitationError, ElicitationExecutor,
    ElicitationOption, ElicitationRecord, ElicitationState, ElicitationTarget, FilingReply,
    HeldCallRequest, WithdrawalReply,
};
pub use hook_plane::SubcHookPlane;
pub use plan_fetch::{SubcPlanAdmitter, SubcPlanFetcher};
pub use scope_describe::{DescribedScope, ModuleScopeDescriber, ScopeDescriber};
pub use session_door::{DoorCoverage, DoorRead, SessionDoorClient, SessionDoorError};
pub use subc_plane::SubcToolPlane;
pub use vault::{VaultAddress, VaultClient, VaultError, VaultGet, VaultToken};
