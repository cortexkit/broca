//! Mode-3 compaction operates on neutral entries, but renders the stored host blocks.
//! The legacy compaction fold and renderer remain independent for recorded requests.

pub use cortexkit_role_compaction as protocol;
/// The view library's render-plan version that a new mode-3 run freezes.
pub use cortexkit_compaction_view::PLAN_VERSION;

pub mod admission;
pub mod authority;
pub mod conditions;
pub mod entries;
pub mod fold;
pub mod host;
pub mod journal;
pub mod metadata;
pub mod projection;
pub mod read;
pub mod received;
pub mod render;
pub mod request;
pub mod runtime;
pub mod session;
pub mod shown;
pub mod state;
pub mod storage;
pub mod systems;
pub mod transcript;
pub mod views;

#[cfg(test)]
mod metadata_tests;
#[cfg(test)]
mod tests;
