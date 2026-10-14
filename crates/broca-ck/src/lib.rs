//! The CK-Message canonical type used by external reduction transforms.

#![forbid(unsafe_code)]

pub mod types;

pub use types::{
    CkMessage, ContentBlock, ContentKind, DecodedRequest, HarnessMeta, MediaBlock, MediaKind,
    MediaSource, MessageOrigin, OpaqueArc, OpaqueArcKind, OpaqueArcRole, OpaqueBlock, OpaqueSource,
    ProviderExtras, ResultBlock, ResultBlockKind, Role, ToolResultOutput, ToolResultOutputKind,
};
