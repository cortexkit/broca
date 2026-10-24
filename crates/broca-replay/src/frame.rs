//! The framed two-lane wire: how an broca payload becomes a subc envelope
//! frame on the DIRECT runner↔provider socket.
//!
//! The envelope is subc's (`subc_protocol::Frame`, the locked 17-byte header).
//! On this DIRECT point-to-point socket — authenticated via subc-transport but
//! NOT relayed through subc-core — we reuse it as the framing codec and
//! repurpose two routing fields, since subc's routing semantics don't apply
//! here:
//!
//! | envelope field | broca meaning |
//! |---|---|
//! | `flags` PRIORITY | [`Lane`] — control=`Interactive`, display=`Background` |
//! | `flags` LAST | final frame of a message/stream |
//! | `flags` BINARY | always 0 (our body is JSON) |
//! | `channel` (u16) | [`StreamGen`] — superseded generation visible without a body parse |
//! | `corr` (u64) | step / approval round-trip correlation |
//! | `type` | always `StreamData`; finality is the LAST flag |
//!
//! The BODY is a [`BodyEnvelope`]: a fixed app-header `{version, run_id,
//! step_id, seq}` followed by the JSON `payload`. `seq` is the per-lane
//! monotonic frame counter used for display-gap detection. [`peek`] reads the
//! envelope-carried metadata WITHOUT deserializing the body, so a writer can
//! priority-schedule and a reader can abandon a dead generation by `stream_gen`
//! before paying for body parse.

use std::{error::Error, fmt};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use subc_protocol::{Flags, Frame, FrameBuildError, FrameType, Priority};

use broca_protocol::ids::{RunId, StepId, StreamGen};

/// Version of the BODY schema this crate freezes. Distinct from the subc
/// envelope version (`subc_protocol::PROTOCOL_VERSION`).
pub const BODY_SCHEMA_VERSION: u16 = 1;

/// The two output lanes. Maps onto the subc envelope PRIORITY field so the
/// writer schedules — and drops lossy display under backpressure — without
/// parsing the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Reliable, reserved-capacity, must-be-complete. The only lane the loop
    /// consumes.
    Control,
    /// Lossy, coalesced, droppable. Passthrough to the caller.
    Display,
}

impl Lane {
    pub fn priority(self) -> Priority {
        match self {
            Lane::Control => Priority::Interactive,
            Lane::Display => Priority::Background,
        }
    }

    /// Recover the lane from the envelope priority. `Passive` is not a lane.
    pub fn from_priority(p: Priority) -> Option<Self> {
        match p {
            Priority::Interactive => Some(Lane::Control),
            Priority::Background => Some(Lane::Display),
            Priority::Passive => None,
        }
    }
}

/// Envelope-carried metadata, recovered without touching the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireMeta {
    pub lane: Lane,
    pub stream_gen: StreamGen,
    pub corr: u64,
    pub last: bool,
}

/// The frame body: a fixed app-header followed by the lane payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BodyEnvelope<T> {
    /// = [`BODY_SCHEMA_VERSION`].
    pub version: u16,
    pub run_id: RunId,
    pub step_id: StepId,
    /// Per-lane monotonic frame counter (display-gap detection).
    pub seq: u64,
    pub payload: T,
}

impl<T> BodyEnvelope<T> {
    /// Build a body at the current schema version.
    pub fn new(run_id: RunId, step_id: StepId, seq: u64, payload: T) -> Self {
        Self {
            version: BODY_SCHEMA_VERSION,
            run_id,
            step_id,
            seq,
            payload,
        }
    }
}

/// Errors encoding/decoding a wire frame.
#[derive(Debug)]
pub enum WireError {
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
    Build(FrameBuildError),
    /// The JSON body uses a schema version this decoder does not understand.
    BodySchemaVersion {
        expected: u16,
        actual: u16,
    },
    /// Not a `StreamData` frame — not part of the two-lane data path.
    NotStreamData(FrameType),
    /// Priority field held `Passive`/reserved — not a valid lane.
    UnschedulablePriority,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Serialize(e) => write!(f, "serialize body: {e}"),
            WireError::Deserialize(e) => write!(f, "deserialize body: {e}"),
            WireError::Build(e) => write!(f, "build frame: {e}"),
            WireError::BodySchemaVersion { expected, actual } => write!(
                f,
                "unsupported body schema version {actual} (expected {expected})"
            ),
            WireError::NotStreamData(ty) => write!(f, "not a StreamData frame: {ty:?}"),
            WireError::UnschedulablePriority => {
                write!(f, "envelope priority is not a valid lane")
            }
        }
    }
}

impl Error for WireError {}

/// Encode a body into a subc `Frame` for the direct socket.
pub fn encode<T: Serialize>(meta: &WireMeta, body: &BodyEnvelope<T>) -> Result<Frame, WireError> {
    let bytes = serde_json::to_vec(body).map_err(WireError::Serialize)?;
    // BINARY=false: the body is JSON, not the subc raw bulk lane.
    let flags = Flags::new(false, meta.lane.priority(), meta.last);
    Frame::build(
        FrameType::StreamData,
        flags,
        meta.stream_gen.0,
        1,
        meta.corr,
        bytes,
    )
    .map_err(WireError::Build)
}

/// Read the envelope-carried metadata WITHOUT parsing the body.
pub fn peek(frame: &Frame) -> Result<WireMeta, WireError> {
    if frame.header.ty != FrameType::StreamData {
        return Err(WireError::NotStreamData(frame.header.ty));
    }
    let prio = frame
        .header
        .flags
        .priority()
        .ok_or(WireError::UnschedulablePriority)?;
    let lane = Lane::from_priority(prio).ok_or(WireError::UnschedulablePriority)?;
    Ok(WireMeta {
        lane,
        stream_gen: StreamGen(frame.header.channel),
        corr: frame.header.corr,
        last: frame.header.flags.is_last(),
    })
}

/// Decode a frame into its metadata + typed body. The caller picks `T` from the
/// lane (returned by [`peek`]) and the socket direction.
pub fn decode<T: DeserializeOwned>(
    frame: &Frame,
) -> Result<(WireMeta, BodyEnvelope<T>), WireError> {
    let meta = peek(frame)?;
    let body: BodyEnvelope<T> =
        serde_json::from_slice(&frame.body).map_err(WireError::Deserialize)?;
    if body.version != BODY_SCHEMA_VERSION {
        return Err(WireError::BodySchemaVersion {
            expected: BODY_SCHEMA_VERSION,
            actual: body.version,
        });
    }
    Ok((meta, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::display::DisplayEvent;
    use broca_protocol::ids::MessageId;

    fn meta(lane: Lane, gen: u16, corr: u64, last: bool) -> WireMeta {
        WireMeta {
            lane,
            stream_gen: StreamGen(gen),
            corr,
            last,
        }
    }

    #[test]
    fn lane_priority_round_trips() {
        for lane in [Lane::Control, Lane::Display] {
            assert_eq!(Lane::from_priority(lane.priority()), Some(lane));
        }
        assert_eq!(Lane::from_priority(Priority::Passive), None);
    }

    #[test]
    fn encode_peek_decode_round_trips() {
        let payload = DisplayEvent::TextDelta {
            mid: None,
            part: None,
            message_id: MessageId("m1".into()),
            tool_call_id: None,
            delta: "hello".into(),
        };
        let body = BodyEnvelope::new(RunId("r1".into()), StepId(7), 42, payload.clone());
        let m = meta(Lane::Display, 3, 99, true);

        let frame = encode(&m, &body).expect("encode");

        // peek recovers envelope meta without body parse.
        let peeked = peek(&frame).expect("peek");
        assert_eq!(peeked, m);
        assert_eq!(peeked.lane, Lane::Display);
        assert_eq!(peeked.stream_gen, StreamGen(3));
        assert_eq!(peeked.corr, 99);
        assert!(peeked.last);

        // envelope-level mapping is correct on the raw header.
        assert_eq!(frame.header.ty, FrameType::StreamData);
        assert_eq!(frame.header.channel, 3);
        assert_eq!(frame.header.flags.priority(), Some(Priority::Background));
        assert!(frame.header.flags.is_last());
        assert!(!frame.header.flags.is_binary());

        let (meta2, decoded): (WireMeta, BodyEnvelope<DisplayEvent>) =
            decode(&frame).expect("decode");
        assert_eq!(meta2, m);
        assert_eq!(decoded.version, BODY_SCHEMA_VERSION);
        assert_eq!(decoded.run_id, RunId("r1".into()));
        assert_eq!(decoded.step_id, StepId(7));
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn decode_rejects_a_mismatched_body_schema_version() {
        let mut body = BodyEnvelope::new(RunId("r".into()), StepId(1), 0, ());
        body.version = BODY_SCHEMA_VERSION + 1;
        let frame = encode(&meta(Lane::Control, 0, 1, false), &body).expect("encode fixture");

        let error = decode::<()>(&frame).expect_err("mismatched version must be rejected");
        assert!(matches!(
            error,
            WireError::BodySchemaVersion {
                expected: BODY_SCHEMA_VERSION,
                actual
            } if actual == BODY_SCHEMA_VERSION + 1
        ));
    }

    #[test]
    fn control_lane_maps_to_interactive_priority() {
        let body = BodyEnvelope::new(RunId("r".into()), StepId(0), 0, ());
        let frame = encode(&meta(Lane::Control, 0, 1, false), &body).expect("encode");
        assert_eq!(frame.header.flags.priority(), Some(Priority::Interactive));
        assert_eq!(peek(&frame).unwrap().lane, Lane::Control);
    }

    #[test]
    fn body_json_shape_is_self_describing() {
        // Pin the on-wire JSON so the contract can't silently drift.
        let payload = DisplayEvent::DisplayGap { dropped: 5 };
        let body = BodyEnvelope::new(RunId("r1".into()), StepId(2), 9, payload);
        let v: serde_json::Value = serde_json::to_value(&body).unwrap();
        assert_eq!(v["version"], 1);
        assert_eq!(v["run_id"], "r1");
        assert_eq!(v["step_id"], 2);
        assert_eq!(v["seq"], 9);
        assert_eq!(v["payload"]["type"], "display_gap");
        assert_eq!(v["payload"]["dropped"], 5);
    }
}
