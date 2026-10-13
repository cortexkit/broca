//! The deterministic delta schedule + encoding + frame build.
//!
//! Body layout (little-endian): `[emit_ns: u64][seq: u64][filler 'x' …]` padded
//! to `payload_bytes`. The first 8 bytes are the monotonic emit timestamp the
//! consumer subtracts from its receive time. The frame is a real subc
//! `StreamData` frame on the display lane (`Background` priority), identical to
//! what arm2 builds, so arm3 − arm2 isolates ONLY process separation.

use subc_protocol::{Flags, Frame, FrameType, Priority};

use crate::clock::monotonic_ns;

/// Drive schedule for an arm.
#[derive(Clone, Copy, Debug)]
pub enum Schedule {
    /// Emit `seq` at absolute time `base + cadence_ns × seq`.
    Fixed { cadence_ns: u64 },
    /// Emit as fast as possible (throughput ceiling + backpressure test).
    Flood,
}

/// Encode a delta body: `emit_ns` + `seq` + filler to `payload_bytes`.
pub fn encode_delta(emit_ns: u64, seq: u64, payload_bytes: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(payload_bytes.max(16));
    v.extend_from_slice(&emit_ns.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    v.resize(payload_bytes.max(16), b'x');
    v
}

/// Read the monotonic emit timestamp from a delta body's first 8 bytes.
pub fn decode_emit_ns(body: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&body[0..8]);
    u64::from_le_bytes(b)
}

/// Build the real subc frame for a delta (display lane, binary body). Shared by
/// arm2 (in-bench) and arm3 (provider) so the wire bytes are identical.
pub fn build_delta_frame(emit_ns: u64, seq: u64, last: bool, payload_bytes: usize) -> Frame {
    let body = encode_delta(emit_ns, seq, payload_bytes);
    // BINARY body, display lane (Background), LAST on the final frame.
    let flags = Flags::new(true, Priority::Background, last);
    Frame::build(FrameType::StreamData, flags, 1, 1, seq, body).expect("delta body within u32")
}

/// Busy-spin to the absolute emit target for `seq` off the monotonic clock
/// (flood returns immediately). Busy-spin — not a platform timer — because the
/// timer's ~1ms granularity is far coarser than the µs hop being measured and
/// would inject its own jitter. The producer owns its task/process, so spinning
/// between sends starves nothing.
pub fn pace(sched: Schedule, base_ns: u64, seq: u64) {
    if let Schedule::Fixed { cadence_ns } = sched {
        let target = base_ns.saturating_add(cadence_ns.saturating_mul(seq));
        while monotonic_ns() < target {
            std::hint::spin_loop();
        }
    }
}
