//! Compatibility types for the retained synchronous compaction client and its WAL replay.
//!
//! The published provider contract no longer defines `compaction.step`. These removed
//! modules retain the 0.2.4 encoding until the client is replaced; they do not implement
//! the published asynchronous protocol and must not enable compaction discovery.

pub mod answer;
pub mod bootstrap;
pub mod describe;
pub mod fence;
pub mod geometry;
pub mod lineage;
pub mod notice;
pub mod points;
pub mod ready;
pub mod setup;
pub mod status;
pub mod view;

pub use cortexkit_role_compaction_provider::{errors, OpRequest, PROVIDES, ROLE, VERSION};

pub mod ops {
    pub use cortexkit_role_compaction_provider::ops::{COMPACTION_SETUP, ROLE_DESCRIBE};
    pub const COMPACTION_STEP: &str = "compaction.step";
}

pub const REQUIRED_OPS: &[&str] = &[
    ops::ROLE_DESCRIBE,
    ops::COMPACTION_SETUP,
    ops::COMPACTION_STEP,
];
