//! Mode-3 compaction operates on neutral entries, but renders the stored host blocks.
//! The legacy compaction fold and renderer remain independent for recorded requests.

pub use cortexkit_role_compaction as protocol;

pub mod entries;
pub mod render;
pub mod runtime;
pub mod state;
pub mod views;

#[cfg(test)]
mod tests;
