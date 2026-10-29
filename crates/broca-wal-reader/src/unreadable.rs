//! Evidence that must be committed with the checkpoint, never priced as zero usage.

use crate::billing::Position;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    PayloadDigest,
    PayloadJson,
    BillingRecord(String),
    UnsupportedContent,
    Header(String),
}

/// Raw bytes may contain private content. Keep them as restricted evidence, not diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreadable {
    pub position: Position,
    pub raw_frame: Vec<u8>,
    pub reason: Reason,
}
