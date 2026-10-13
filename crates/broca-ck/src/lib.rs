//! The CK-Message canonical type + the lift/project codec between it and the owned
//! broca canonical message.
//!
//! `CkMessage` is the harness-agnostic message an external reduction transform operates on
//! (see [`types`]); [`lift`] maps the owned canonical prompt into it and [`project`] maps a
//! (possibly transformed) array back. The load-bearing property is RENDER byte-identity for
//! untransformed content — see [`codec`].

#![forbid(unsafe_code)]

pub mod codec;
pub mod types;

pub use codec::{lift, project, ProjectError};
pub use types::{
    CkMessage, ContentBlock, ContentKind, DecodedRequest, HarnessMeta, MediaBlock, MediaKind,
    MediaSource, MessageOrigin, OpaqueArc, OpaqueArcKind, OpaqueArcRole, OpaqueBlock, OpaqueSource,
    ProviderExtras, ResultBlock, ResultBlockKind, Role, ToolResultOutput, ToolResultOutputKind,
};
