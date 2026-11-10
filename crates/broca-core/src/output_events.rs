//! Events passed from a run loop to its caller.

use broca_protocol::{ControlUnit, DisplayEvent};

/// One event on the runner→caller OutputStream. Display = lossy passthrough;
/// Control = the reliable assembled units.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputEvent {
    /// Queue-ordered transcript position for the following step's advisory display output.
    DisplayContext {
        ordinal: Option<u64>,
    },
    Display(DisplayEvent),
    Control(ControlUnit),
}
