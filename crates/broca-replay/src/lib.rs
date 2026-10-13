//! Shared (A)-bench replay fixture.
//!
//! Both the bench consumer (arm2 socket producer + all consumers) and the
//! provider `--bench-replay` producer (arm3) use THESE functions, so the two
//! ends can't drift on the delta encoding, the frame build, the clock, or the
//! pacing — a drift there would silently corrupt the cross-process measurement.
#![forbid(unsafe_code)]

pub mod clock;
pub mod frame;
pub mod replay;

pub use clock::monotonic_ns;
pub use frame::{
    decode, encode, peek, BodyEnvelope, Lane, WireError, WireMeta, BODY_SCHEMA_VERSION,
};
pub use replay::{build_delta_frame, decode_emit_ns, encode_delta, pace, Schedule};
