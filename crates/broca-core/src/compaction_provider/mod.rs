//! The published `compaction-provider/v1` role (cortexkit-role-compaction-provider 0.3.x),
//! re-exported under the path broca's crates already use.
//!
//! The role is asynchronous: after Setup the provider learns what the runner wrote from
//! advance notices, and the runner applies the provider's published views at step
//! boundaries. Earlier drafts had a synchronous per-step `compaction.step` call; broca no
//! longer has a client for it. WAL records that client wrote are still decoded and replayed
//! (see [`crate::compaction::CompactionState::fold`]), so an old session renders the same
//! bytes it rendered before.

pub use cortexkit_role_compaction_provider::*;
