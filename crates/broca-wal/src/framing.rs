//! On-disk record framing.
//!
//! ```text
//! frame   := [len: u32 LE][ver: u8][seq: u64 LE][fence: u64 LE][digest: 32][payload: len]
//! record  := frame(ver=1, seq>=1, fence=lease tenure, payload=WalRecord JSON)
//! lineage := frame(ver=2, seq=0, fence=0, payload=16 opaque identity bytes)
//! gated   := frame(ver=3, seq>=1, fence=lease tenure,
//!                  payload={"requires": [feature name, ...], "record": WalRecord JSON})
//! digest  := SHA-256 over (ver ‖ seq ‖ fence ‖ payload)
//! ```
//!
//! A gated frame carries a record that a reader must understand before it may use it: a new
//! record kind, a new value in a closed set, or a new field whose meaning cannot be ignored.
//! `requires` names the features the record uses. A reader checks every name against
//! [`KNOWN_FEATURES`] BEFORE it interprets the record, and refuses the whole WAL with
//! [`DecodeStep::NewerWriter`] if any is missing. Skipping the record would silently lose it,
//! and calling it corruption would invite a repair that destroys a newer build's history.
//!
//! The digest covers `ver`, so a gated frame relabelled as an ordinary one fails its digest
//! rather than being read as a record whose requirements were never checked. Builds that
//! predate gated frames refuse `ver` 3 by name ("unsupported WAL framing version 3"), which
//! is what makes rolling back to them safe: they stop instead of skipping or misreading a record
//! a newer build wrote.
//!
//! SHA-256 (not crc32) because this is a source-of-truth log; records are
//! low-volume (off the hot path) so a strong digest is cheap. `len` is capped +
//! validated before allocation, so a corrupt length can't force a huge alloc.

use serde::{de::IgnoredAny, Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::WalRecord;

/// Fixed header bytes: len(4) + ver(1) + seq(8) + fence(8) + digest(32).
pub const HEADER_LEN: usize = 4 + 1 + 8 + 8 + 32;
const DIGEST_LEN: usize = 32;

/// Max payload accepted before allocation. 64 MiB — an assembled unit is far
/// smaller; this guards a corrupt `len` from forcing an unbounded alloc.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// A record as read off disk: its metadata + decoded body.
#[derive(Debug, Clone, PartialEq)]
pub struct FramedRecord {
    pub seq: u64,
    pub fence: u64,
    pub record: WalRecord,
}

/// Opaque durable identity stored inside a WAL independently of its record sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalLineageId([u8; 16]);

impl WalLineageId {
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl std::fmt::Display for WalLineageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
            b[12], b[13], b[14], b[15]
        )
    }
}

#[derive(Debug)]
pub enum FrameError {
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
    /// `len` exceeded `MAX_PAYLOAD_LEN`.
    PayloadTooLarge {
        len: u32,
    },
    /// A fully-read record whose digest didn't verify — CORRUPTION (fail loud).
    DigestMismatch {
        seq: u64,
    },
    /// An incomplete frame has later intact data, so its damage is not confined
    /// to a discardable final append.
    NonTailIncompleteFrame,
    /// Unsupported framing version byte.
    UnsupportedVersion {
        ver: u8,
    },
    /// A lineage metadata frame used an invalid reserved envelope.
    InvalidLineageFrame {
        len: u32,
        seq: u64,
        fence: u64,
    },
    /// A writer asked to gate a record on a feature name outside the capability grammar
    /// (`[a-z0-9-]+/v<digits>`). Refused at write time so no frame carries a name that no
    /// reader could ever list in its known-name table.
    InvalidFeatureName {
        name: String,
    },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Serialize(e) => write!(f, "serialize record: {e}"),
            FrameError::Deserialize(e) => write!(f, "deserialize record: {e}"),
            FrameError::PayloadTooLarge { len } => {
                write!(
                    f,
                    "record payload {len} exceeds the {MAX_PAYLOAD_LEN}-byte cap"
                )
            }
            FrameError::DigestMismatch { seq } => {
                write!(f, "record seq={seq} failed its digest — WAL corruption")
            }
            FrameError::NonTailIncompleteFrame => {
                write!(f, "incomplete frame before intact data")
            }
            FrameError::UnsupportedVersion { ver } => {
                write!(f, "unsupported WAL framing version {ver}")
            }
            FrameError::InvalidLineageFrame { len, seq, fence } => write!(
                f,
                "invalid WAL lineage frame (len={len}, seq={seq}, fence={fence})"
            ),
            FrameError::InvalidFeatureName { name } => write!(
                f,
                "invalid WAL feature name {name:?} (expected [a-z0-9-]+/v<number>)"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

/// Framing version byte (the on-disk envelope; distinct from the record schema
/// version inside the payload).
pub const FRAMING_VERSION: u8 = 1;
const LINEAGE_FRAMING_VERSION: u8 = 2;
const LINEAGE_PAYLOAD_LEN: u32 = 16;
/// Framing version of a gated record: its payload is `{"requires": [...], "record": ...}`,
/// and a reader must know every feature name in `requires` before it interprets `record`.
pub const GATED_FRAMING_VERSION: u8 = 3;

/// The feature names this build understands in a gated frame's `requires` list.
///
/// THIS IS THE ONLY TABLE. Every reader path (replay, the anchored range read behind
/// `session.read`, projection, the control lane, fold eligibility, `wal-verify`, archive reads)
/// decodes through [`decode_next`], which consults exactly this list, so no path can accept a
/// record another path refuses.
///
/// A name is added only by the release that interprets it, and must be in a deployed reader
/// before any build writes it: a WAL written with a name missing from this list is refused
/// as [`DecodeStep::NewerWriter`]. `ck-broca --wal-known-features` prints this list so
/// placement tooling can refuse to install a build that cannot read what is already on disk.
pub const KNOWN_FEATURES: &[&str] = &[
    AGENT_RUN_SCOPE_FEATURE,
    ARCHIVE_INDEX_V2_FEATURE,
    COMPACTION_VIEW_FEATURE,
    COMPACTION_WAIT_FEATURE,
    COMPACTION_FEATURE,
    CONTEXT_CEILING_FEATURE,
    DEFERRED_CALL_FEATURE,
    DELIVERY_ORDER_FEATURE,
    DISPATCH_MODULE_FEATURE,
    FLOW_SCOPE_FEATURE,
    GOOGLE_RPC_ERROR_DETAILS_FEATURE,
    INPUT_TOKEN_CAP_FEATURE,
    OPENROUTER_REASONING_FEATURE,
    PLAN_MANIFEST_FEATURE,
    PROVIDER_TEXT_STATE_FEATURE,
    RESTART_PAUSE_FEATURE,
    RETENTION_FEATURE,
    RUNNER_TAGS_FEATURE,
    RUN_CANCEL_AT_FEATURE,
    SCOPE_FEATURE,
    SESSION_CLASS_FEATURE,
    STEER_QUEUE_FEATURE,
    STEP_TRANSFORMS_FEATURE,
    STRUCTURED_OUTPUT_FEATURE,
    STRUCTURED_OUTPUT_DOWNGRADE_FEATURE,
];

/// Google RPC quota details on a stored provider error must not be silently discarded on read.
pub const GOOGLE_RPC_ERROR_DETAILS_FEATURE: &str = "google-rpc-error-details/v1";

/// Text-part state and the frozen codec policy are required for faithful provider replay.
pub const PROVIDER_TEXT_STATE_FEATURE: &str = "provider-text-state/v1";

/// OpenRouter-tagged reasoning and its frozen echo policy cannot be read faithfully by older
/// builds. This is distinct from text-part phase/signature state in provider-text-state/v1.
pub const OPENROUTER_REASONING_FEATURE: &str = "openrouter-reasoning/v1";

/// Compaction's issued fence, applied view and other answers. Missing it must refuse the
/// WAL rather than lose an applied view or allow a superseded request to apply on replay.
pub const COMPACTION_FEATURE: &str = "compaction/v1";

/// Durable compaction wait exits, including terminal intents that recovery must seal.
pub const COMPACTION_WAIT_FEATURE: &str = "compaction-wait/v1";

/// Asynchronous compaction (compaction-provider/v1 0.3): a provider view applied at a step
/// boundary together with how it was applied (as published, or with the companion strip of
/// signed reasoning), the advance notices recorded before they are published, and the ceiling
/// policy frozen from Setup. A build that skipped any of them would render a resumed request
/// without the applied view, reuse a notice sequence number, or forget the ceiling, so all
/// three are must-understand.
pub const COMPACTION_VIEW_FEATURE: &str = "compaction-view/v1";

/// Bounded ceiling waits and send decisions, including a non-error terminal state. Ignoring
/// them could repeat a wait after a crash or send a request the runner durably refused.
pub const CONTEXT_CEILING_FEATURE: &str = "context-ceiling/v1";

/// Not a WAL frame feature: no frame ever requires it, so its entry here is inert for the
/// decoder. It is listed because this table is also what `--wal-known-features` reports to
/// `scripts/place-release.sh`, which refuses to install a build whose table does not cover
/// the machine's feature floor (see `floor.rs`). The floor answers "can this build read the
/// data on this machine", archives included. A build that seals archive containers with index
/// format v2 adds this name to the floor first, so placement then refuses any older build:
/// one that cannot recognise a v2 index treats it as damaged, and so reads every such
/// container by full header scans and without the index's cross-check of member headers
/// (see `archive.rs`).
pub const ARCHIVE_INDEX_V2_FEATURE: &str = "archive-index/v2";

/// The feature a `RunStarted` carrying the session's frozen plan manifest requires
/// (`WalRecord::RunStarted::manifest`). A reader without it must refuse such a WAL: skipping the
/// field would render the session without the tools and system text it was admitted with, or
/// fetch them again and get different ones.
pub const PLAN_MANIFEST_FEATURE: &str = "plan-manifest/v1";

/// A durable graceful-restart pause preserves an unfinished episode for boot recovery.
pub const RESTART_PAUSE_FEATURE: &str = "restart-pause/v1";

/// Retention policy and queued/steered activity times affect expiry and deletion. A reader
/// must not ignore them and keep content indefinitely. Reader builds enforce retention across
/// admission, projection, reads and deletion; production sends stay off until capability rollout.
pub const RETENTION_FEATURE: &str = "retention/v1";

/// Runner tags: the per-part tag numbers and per-message origins a session's records carry,
/// and a run start frozen to render them (`RunConfig::render_tags`). A reader that skipped them
/// would serve the session's reads without tags, let a later write reuse a number, or render a
/// run's requests without the tags it was admitted to show. Every record of a session admitted
/// by a tag-writing build carries them; sessions admitted earlier keep their exact bytes.
pub const RUNNER_TAGS_FEATURE: &str = "runner-tags/v1";

/// Pending cancellation changes recovery's next action, so a reader may not skip it.
pub const RUN_CANCEL_AT_FEATURE: &str = "run-cancel-at/v1";

/// Labels fixed on the first send govern later admission and backup placement.
pub const SESSION_CLASS_FEATURE: &str = "session-class/v1";

/// Ignoring a session input cap would authorize paid model steps the caller forbade.
pub const INPUT_TOKEN_CAP_FEATURE: &str = "input-token-cap/v1";

/// A run frozen with structured output renders its schema into every request and ends when the
/// model calls `submit_result`. A reader that skipped the frozen settings would resume the run
/// without the schema or the submit tool, and would dispatch the calls a submit skipped.
pub const STRUCTURED_OUTPUT_FEATURE: &str = "structured-output/v1";

/// A schema compilation refusal changes every subsequent request. Older readers must not
/// skip the downgrade and resume sending the strict grammar the provider already refused.
pub const STRUCTURED_OUTPUT_DOWNGRADE_FEATURE: &str = "structured-output-downgrade/v1";

/// The feature a `RunStarted` carrying the session's frozen scope requires
/// (`WalRecord::RunStarted::scope`). A reader without it must refuse such a WAL: skipping the
/// field would let any caller drive a session only its scope's owner may drive.
pub const SCOPE_FEATURE: &str = "scope/v1";

/// Frozen automation identity affects admission. A scope-only reader would ignore the sender
/// constraint and let the owner agent drive a carrier's session, so it must refuse this WAL.
pub const FLOW_SCOPE_FEATURE: &str = "flow-scopes/v1";

/// A scope's `run_id` and `agent_id`, fixed on `RunStarted`. Older readers must refuse these
/// records: ignoring the identifiers would admit an agent-run session as an ordinary session
/// driven by its owner, instead of a model-only session driven by the reserved Basal carrier.
pub const AGENT_RUN_SCOPE_FEATURE: &str = "agent-run-scopes/v1";

/// The feature a `Steered` record requires (`WalRecord::Steered`): a message a caller added to
/// a running turn. A reader without it must refuse such a WAL: skipping the record would drop a
/// user message from the transcript and render a different prompt than the one the run sent.
pub const STEER_QUEUE_FEATURE: &str = "steer-queue/v1";

/// The feature a `ToolDispatchIntent` naming the module its call was dispatched to, or the
/// schema it was dispatched against, requires (`WalRecord::ToolDispatchIntent::module_id` and
/// `::schema_pin`). A reader without it must refuse such a WAL: skipping the fields would
/// report a dispatched call as never dispatched, and lose which schema the provider was held to.
pub const DISPATCH_MODULE_FEATURE: &str = "dispatch-module/v1";

/// Recorded step-transform output and frozen declarations must survive rollback. A reader
/// without this feature must stop instead of continuing a session without its hook outputs.
pub const STEP_TRANSFORMS_FEATURE: &str = "step-transforms/v1";

/// The feature every record of a tool call held for a human's approval requires: the
/// `tool_call_deferred`, `approval_recorded` and `awaiting_approval` records, the three late
/// records, `ToolDispatchIntent.approval_digest` and `.call_key`, and
/// `ToolResult.withdraw_elicitation` and `.provider_held` (see
/// [`WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping
/// them would dispatch a call no human approved, dispatch an approved call twice, or forget a
/// question or a provider-held call it owes a withdrawal.
///
/// Reader builds interpret these records in replay, store projection and session reads. The
/// production-writer source fence keeps them reader-only until a writer release is enabled.
pub const DEFERRED_CALL_FEATURE: &str = "deferred-call/v1";

/// The feature every record of a send with an ordered `delivery` list, a `dedupe_key` or an
/// `expires_at_ms` requires: `delivery_ordered`, `send_ended` and `send_delivered` (see
/// [`WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping them
/// would start a run from a queued send that may never start one, deliver a send that was
/// replaced or expired, or render a prompt without a send that was delivered into it.
pub const DELIVERY_ORDER_FEATURE: &str = "delivery-order/v1";

/// A feature name that test builds treat as known, in addition to [`KNOWN_FEATURES`].
///
/// Tests of the paths that copy or rewrite WAL bytes (fold, unfold, archive extraction,
/// torn-tail repair, maintenance) need a gated frame this build can READ, and the shipped table
/// is empty. Those paths live in other crates, where this crate's `cfg(test)` does not apply,
/// so the name is also enabled by the `test-known-feature` cargo feature, which only
/// dev-dependencies turn on. It is never in [`KNOWN_FEATURES`], so `--wal-known-features` and
/// the placement floor never see it.
#[cfg(any(test, feature = "test-known-feature"))]
pub const TEST_KNOWN_FEATURE: &str = "test-only-known/v1";

#[cfg(any(test, feature = "test-known-feature"))]
const TEST_KNOWN_FEATURES: &[&str] = &[TEST_KNOWN_FEATURE];
#[cfg(not(any(test, feature = "test-known-feature")))]
const TEST_KNOWN_FEATURES: &[&str] = &[];

/// Feature names a test has asked the decoder to treat as UNKNOWN although [`KNOWN_FEATURES`]
/// lists them, so the test can decode as a build released before those features existed would
/// (see [`forget_feature_for_test`]).
#[cfg(any(test, feature = "test-known-feature"))]
static FORGOTTEN_FOR_TEST: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());

/// Test builds only: make every decode in this process treat `name` as unknown until the
/// returned guard drops, as a build released before `name` existed would.
///
/// The setting is process-wide, so a test using it must be the only test in its binary (or
/// the only one decoding a WAL while the guard lives).
#[cfg(any(test, feature = "test-known-feature"))]
pub fn forget_feature_for_test(name: &'static str) -> ForgottenFeature {
    FORGOTTEN_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(name);
    ForgottenFeature(name)
}

/// Restores a name [`forget_feature_for_test`] hid when dropped.
#[cfg(any(test, feature = "test-known-feature"))]
pub struct ForgottenFeature(&'static str);

#[cfg(any(test, feature = "test-known-feature"))]
impl Drop for ForgottenFeature {
    fn drop(&mut self) {
        let mut forgotten = FORGOTTEN_FOR_TEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(at) = forgotten.iter().position(|name| *name == self.0) {
            forgotten.remove(at);
        }
    }
}

#[cfg(any(test, feature = "test-known-feature"))]
fn forgotten_for_test(name: &str) -> bool {
    FORGOTTEN_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(&name)
}

#[cfg(not(any(test, feature = "test-known-feature")))]
fn forgotten_for_test(_name: &str) -> bool {
    false
}

// An optimised build is a build that could be placed. A test-only name must never make one
// accept a frame that a real build of the same source would refuse as a newer writer.
#[cfg(all(feature = "test-known-feature", not(debug_assertions)))]
compile_error!(
    "broca-wal's `test-known-feature` is for test builds only; it must not reach a release build"
);

/// The envelope a gated frame's payload is written as. Field order is part of the byte
/// layout: `requires` first, then the record.
#[derive(Serialize)]
struct GatedEnvelopeOut<'a> {
    requires: &'a [&'a str],
    record: &'a WalRecord,
}

/// First pass over a gated payload: read `requires` and leave the record uninterpreted.
///
/// The record is deliberately not parsed here. A record that needs a feature this build lacks
/// may use values this build's types reject, and that failure would surface as corruption
/// instead of as the newer-writer refusal it really is.
#[derive(Deserialize)]
struct GatedRequires {
    requires: Vec<String>,
    #[allow(dead_code)]
    record: IgnoredAny,
}

/// Read requirements from an already verified, complete prefix. Keeping the
/// original envelope matters: an explicit gate may require more than the
/// decoded record's current `required_features()` implementation does.
pub(crate) fn verified_prefix_features(mut bytes: &[u8]) -> Result<Vec<String>, FrameError> {
    let mut features = std::collections::BTreeSet::new();
    while !bytes.is_empty() {
        let Some(header) = bytes.get(..HEADER_LEN) else {
            return Err(FrameError::NonTailIncompleteFrame);
        };
        let len = u32::from_le_bytes(header[..4].try_into().expect("four-byte length")) as usize;
        let Some(frame) = bytes.get(..HEADER_LEN + len) else {
            return Err(FrameError::NonTailIncompleteFrame);
        };
        if header[4] == 3 {
            let envelope: GatedRequires =
                serde_json::from_slice(&frame[HEADER_LEN..]).map_err(FrameError::Deserialize)?;
            features.extend(envelope.requires);
        }
        bytes = &bytes[frame.len()..];
    }
    Ok(features.into_iter().collect())
}

/// Second pass over a gated payload, run only once every required feature is known.
#[derive(Deserialize)]
struct GatedRecord {
    record: WalRecord,
}

/// Whether `name` is in the capability grammar: one or more of `[a-z0-9-]`, then `/v`, then
/// one or more ASCII digits.
fn is_feature_name(name: &str) -> bool {
    let Some((stem, version)) = name.split_once("/v") else {
        return false;
    };
    !stem.is_empty()
        && stem
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_digit())
}

fn compute_digest(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> [u8; DIGEST_LEN] {
    let mut h = Sha256::new();
    h.update([ver]);
    h.update(seq.to_le_bytes());
    h.update(fence.to_le_bytes());
    h.update(payload);
    h.finalize().into()
}

/// Verify the fixed frame envelope without interpreting its version or payload.
/// Intact bytes that this reader rejects may belong to a newer writer, not a crash.
pub(crate) fn digest_verifies(buf: &[u8]) -> bool {
    if buf.len() < HEADER_LEN {
        return false;
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    let Some(total) = HEADER_LEN.checked_add(len) else {
        return false;
    };
    if buf.len() < total {
        return false;
    }
    let seq = u64::from_le_bytes(buf[5..13].try_into().unwrap());
    let fence = u64::from_le_bytes(buf[13..21].try_into().unwrap());
    compute_digest(buf[4], seq, fence, &buf[HEADER_LEN..total]).as_slice() == &buf[21..53]
}

/// Encode a record into its complete framed bytes.
///
/// A [`WalRecord::Unknown`] is refused with `FrameError::Serialize`: it holds none of the
/// original record's content, so writing it would replace a newer build's record with an
/// empty placeholder.
pub fn encode(seq: u64, fence: u64, record: &WalRecord) -> Result<Vec<u8>, FrameError> {
    encode_with_requires(seq, fence, &[], record)
}

/// Encode a record that may need features an older reader lacks.
///
/// The record's own [`WalRecord::required_features`] are always added to `requires`, so a
/// must-understand value can never land in a frame an older build reads without checking.
///
/// With nothing required this is an ordinary version-1 frame, byte for byte what every
/// deployed build reads. Otherwise it writes a version-3 frame whose payload is
/// `{"requires": [...], "record": ...}`, which a reader lacking one of the names refuses as
/// [`DecodeStep::NewerWriter`]. Names are sorted and deduplicated so the same record always
/// produces the same bytes, and each must be in the capability grammar.
pub fn encode_with_requires(
    seq: u64,
    fence: u64,
    requires: &[&str],
    record: &WalRecord,
) -> Result<Vec<u8>, FrameError> {
    let own = record.required_features();
    let requires: Vec<&str> = requires.iter().chain(own.iter()).copied().collect();
    if requires.is_empty() {
        let payload = serde_json::to_vec(record).map_err(FrameError::Serialize)?;
        return frame(FRAMING_VERSION, seq, fence, &payload);
    }
    if let Some(bad) = requires.iter().find(|name| !is_feature_name(name)) {
        return Err(FrameError::InvalidFeatureName {
            name: (*bad).to_string(),
        });
    }
    let mut names = requires;
    names.sort_unstable();
    names.dedup();
    let payload = serde_json::to_vec(&GatedEnvelopeOut {
        requires: &names,
        record,
    })
    .map_err(FrameError::Serialize)?;
    frame(GATED_FRAMING_VERSION, seq, fence, &payload)
}

/// Frame an already-serialized record payload, with the same header and digest as
/// [`encode`].
///
/// The engine never calls this directly; every record it writes goes through [`encode`].
/// It is public so a test can write a frame this build has no type for, such as a record
/// type added by a newer build, through the real framing rather than a copy of it.
pub fn encode_payload(seq: u64, fence: u64, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    frame(FRAMING_VERSION, seq, fence, payload)
}

fn frame(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    let len =
        u32::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge { len: u32::MAX })?;
    if len > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge { len });
    }
    let digest = compute_digest(ver, seq, fence, payload);

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.push(ver);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&fence.to_le_bytes());
    out.extend_from_slice(&digest);
    out.extend_from_slice(payload);
    Ok(out)
}

/// Total bytes of a lineage metadata frame: the standard header plus the 16-byte identity.
///
/// Fixed by construction — the payload is a `WalLineageId`, which is exactly
/// `LINEAGE_PAYLOAD_LEN` bytes — which is what lets `encode_lineage` return an array rather
/// than allocating.
/// Fixed byte length of a lineage frame, for bounded identity checks without log replay.
pub const LINEAGE_FRAME_LEN: usize = HEADER_LEN + LINEAGE_PAYLOAD_LEN as usize;

/// Encode lineage metadata in the framing stream without consuming a record sequence number.
///
/// Returns a FIXED ARRAY rather than a `Vec` because this runs on EVERY append, not only on the
/// legacy-mint path that needs it: `file_wal.rs` calls it unconditionally and only afterwards
/// decides whether the bytes are used. A heap allocation per WAL append is pure waste for a
/// frame whose length is known at compile time.
pub(crate) fn encode_lineage(lineage_id: WalLineageId) -> [u8; LINEAGE_FRAME_LEN] {
    let payload = lineage_id.as_bytes();
    let digest = compute_digest(LINEAGE_FRAMING_VERSION, 0, 0, &payload);

    let mut out = [0u8; LINEAGE_FRAME_LEN];
    out[0..4].copy_from_slice(&LINEAGE_PAYLOAD_LEN.to_le_bytes());
    out[4] = LINEAGE_FRAMING_VERSION;
    out[5..13].copy_from_slice(&0u64.to_le_bytes());
    out[13..21].copy_from_slice(&0u64.to_le_bytes());
    out[21..53].copy_from_slice(&digest);
    out[53..69].copy_from_slice(&payload);
    out
}

/// Outcome of decoding one frame from a byte cursor.
pub enum DecodeStep<T = FramedRecord> {
    /// A complete, digest-verified record + how many bytes it consumed. Boxed: the
    /// decoded record dwarfs the other variants and this is a transient return
    /// value, so indirection keeps the enum small without a cold-path penalty.
    Record { framed: Box<T>, consumed: usize },
    /// A complete, digest-verified lineage metadata frame.
    Lineage {
        lineage_id: WalLineageId,
        consumed: usize,
    },
    /// EOF mid-frame (an incomplete trailing append) — the TORN TAIL. The caller
    /// discards from here (it was never durably committed). `available` bytes
    /// remained, fewer than needed.
    TornTail,
    /// A fully-present-but-invalid frame — CORRUPTION; the caller FAILS LOUD.
    Corrupt(FrameError),
    /// A complete, digest-verified gated frame that requires features missing from
    /// [`KNOWN_FEATURES`]: a newer build wrote it. The caller refuses the WHOLE WAL by this
    /// name. It is not corruption, so nothing may repair, truncate or fold past it, and it is
    /// not skippable, because the record may change the meaning of everything after it.
    NewerWriter {
        ver: u8,
        /// The unknown names, sorted and deduplicated.
        missing: Vec<String>,
    },
}

/// Decode the next record or lineage metadata frame starting at `buf[0]`. Distinguishes a torn
/// tail (incomplete final append → discard) from corruption (a complete frame that fails its
/// digest/decode → fail loud).
pub fn decode_next(buf: &[u8]) -> DecodeStep {
    decode_next_with(buf, KNOWN_FEATURES)
}

/// [`decode_next`] against an explicit known-name table. Production always passes
/// [`KNOWN_FEATURES`]; tests pass a table with an entry the shipped one lacks, to prove a
/// known name decodes without putting a name in the production table.
fn decode_next_with(buf: &[u8], known: &[&str]) -> DecodeStep {
    decode_projected_with(buf, known, |seq, fence, ver, payload| {
        let record = if ver == GATED_FRAMING_VERSION {
            serde_json::from_slice::<GatedRecord>(payload).map(|envelope| envelope.record)
        } else {
            serde_json::from_slice::<WalRecord>(payload)
        }?;
        Ok(FramedRecord { seq, fence, record })
    })
}

/// Project a verified frame without constructing a full record. Feature requirements are
/// still checked before the projection, so an admission index cannot skip a newer writer.
pub(crate) fn decode_projected<T>(
    buf: &[u8],
    project: impl FnOnce(u64, u64, u8, &[u8]) -> Result<T, serde_json::Error>,
) -> DecodeStep<T> {
    decode_projected_with(buf, KNOWN_FEATURES, project)
}

fn decode_projected_with<T>(
    buf: &[u8],
    known: &[&str],
    project: impl FnOnce(u64, u64, u8, &[u8]) -> Result<T, serde_json::Error>,
) -> DecodeStep<T> {
    if buf.len() < HEADER_LEN {
        return DecodeStep::TornTail; // not even a full header → torn
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let ver = buf[4];
    let seq = u64::from_le_bytes(buf[5..13].try_into().unwrap());
    let fence = u64::from_le_bytes(buf[13..21].try_into().unwrap());
    let digest = &buf[21..53];

    if ver != FRAMING_VERSION && ver != LINEAGE_FRAMING_VERSION && ver != GATED_FRAMING_VERSION {
        return DecodeStep::Corrupt(FrameError::UnsupportedVersion { ver });
    }
    if len > MAX_PAYLOAD_LEN {
        return DecodeStep::Corrupt(FrameError::PayloadTooLarge { len });
    }
    let total = HEADER_LEN + len as usize;
    if buf.len() < total {
        return DecodeStep::TornTail; // header present, payload incomplete → torn
    }
    let payload = &buf[HEADER_LEN..total];
    let expected = compute_digest(ver, seq, fence, payload);
    if expected.as_slice() != digest {
        // A FULLY-present frame whose digest fails = corruption, not a torn write.
        return DecodeStep::Corrupt(FrameError::DigestMismatch { seq });
    }

    if ver == LINEAGE_FRAMING_VERSION {
        if len != LINEAGE_PAYLOAD_LEN || seq != 0 || fence != 0 {
            return DecodeStep::Corrupt(FrameError::InvalidLineageFrame { len, seq, fence });
        }
        let mut bytes = [0u8; LINEAGE_PAYLOAD_LEN as usize];
        bytes.copy_from_slice(payload);
        return DecodeStep::Lineage {
            lineage_id: WalLineageId::from_bytes(bytes),
            consumed: total,
        };
    }

    if ver == GATED_FRAMING_VERSION {
        let requires = match serde_json::from_slice::<GatedRequires>(payload) {
            Ok(envelope) => envelope.requires,
            Err(e) => return DecodeStep::Corrupt(FrameError::Deserialize(e)),
        };
        let mut missing: Vec<String> = requires
            .into_iter()
            .filter(|name| {
                (!known.contains(&name.as_str()) || forgotten_for_test(name))
                    && !TEST_KNOWN_FEATURES.contains(&name.as_str())
            })
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            return DecodeStep::NewerWriter { ver, missing };
        }
    }
    match project(seq, fence, ver, payload) {
        Ok(record) => DecodeStep::Record {
            framed: Box::new(record),
            consumed: total,
        },
        Err(e) => DecodeStep::Corrupt(FrameError::Deserialize(e)),
    }
}

/// Serializes the tests in this crate that hide a feature with [`forget_feature_for_test`]
/// against tests that decode frames requiring that feature. The hiding is process-wide, so
/// without this a test asserting that a known feature decodes can run while another test hides
/// it, and fail at random.
#[cfg(test)]
fn lock_feature_table_for_test() -> std::sync::MutexGuard<'static, ()> {
    static FEATURE_TABLE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FEATURE_TABLE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod deferred_call_tests;

#[cfg(test)]
mod openrouter_reasoning_tests;

#[cfg(test)]
mod retention_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::StepId;

    fn rec(n: u64) -> WalRecord {
        WalRecord::TurnFinished { step_id: StepId(n) }
    }

    #[test]
    fn round_trip() {
        let bytes = encode(7, 3, &rec(1)).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.seq, 7);
                assert_eq!(framed.fence, 3);
                assert_eq!(framed.record, rec(1));
            }
            _ => panic!("expected a record"),
        }
    }

    /// A digest-valid frame carrying a record type from a newer build decodes as a record, not
    /// as corruption, and the framing refuses to write the lossy placeholder back out.
    #[test]
    fn a_frame_with_an_unknown_record_type_decodes_and_is_never_re_encoded() {
        let bytes = encode_payload(
            4,
            2,
            br#"{"type":"attempt_digest_from_a_newer_build","attempt":1}"#,
        )
        .unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!((framed.seq, framed.fence), (4, 2));
                assert_eq!(framed.record, WalRecord::Unknown);
            }
            _ => panic!("expected a record"),
        }
        assert!(matches!(
            encode(5, 2, &WalRecord::Unknown),
            Err(FrameError::Serialize(_))
        ));
    }

    #[test]
    fn lineage_metadata_round_trips_outside_the_record_sequence() {
        let lineage_id = WalLineageId::from_bytes([0x5a; 16]);
        let bytes = encode_lineage(lineage_id);
        match decode_next(&bytes) {
            DecodeStep::Lineage {
                lineage_id: decoded,
                consumed,
            } => {
                assert_eq!(decoded, lineage_id);
                assert_eq!(consumed, bytes.len());
            }
            _ => panic!("expected lineage metadata"),
        }
    }

    /// Pins the lineage frame's EXACT WIRE BYTES for a known identity.
    ///
    /// The round-trip test above cannot catch a layout change, because it encodes and decodes
    /// with the same code: reorder two fields and both halves agree with each other while every
    /// WAL already on disk becomes unreadable. This asserts the bytes themselves, so the layout
    /// is pinned independently of the encoder that produces it.
    ///
    /// If this fails, the framing changed. That is a DURABILITY BREAK for every existing WAL,
    /// not a golden to refresh -- fix the encoder rather than the expectation.
    #[test]
    fn lineage_frame_wire_bytes_are_pinned() {
        let bytes = encode_lineage(WalLineageId::from_bytes([0x5a; 16]));

        assert_eq!(bytes.len(), LINEAGE_FRAME_LEN);
        assert_eq!(LINEAGE_FRAME_LEN, 69, "header 53 + identity 16");

        // Field-by-field, at fixed offsets, so a failure names WHICH field moved.
        assert_eq!(&bytes[0..4], &16u32.to_le_bytes(), "payload length");
        assert_eq!(bytes[4], 2, "lineage framing version");
        assert_eq!(
            &bytes[5..13],
            &0u64.to_le_bytes(),
            "seq: consumes no record number"
        );
        assert_eq!(&bytes[13..21], &0u64.to_le_bytes(), "fence");
        assert_eq!(&bytes[53..69], &[0x5a; 16], "identity payload");

        // THE DIGEST IS A FROZEN LITERAL, COMPUTED OUTSIDE THIS CRATE.
        //
        // It was first asserted as `compute_digest(2, 0, 0, &[0x5a; 16])`, reasoning that this
        // test is about LAYOUT and the digest only needs to be non-blank. That was wrong, and it
        // reintroduced the very defect this test exists to close: the encoder, the decoder AND
        // that assertion all call `compute_digest`, so a change to its domain moved all three
        // together. Swapping seq/fence inside the hash left the ENTIRE broca-wal suite green
        // while every frame already on disk would decode as DigestMismatch.
        //
        // sha256 over: version byte 0x02, seq 0u64 LE, fence 0u64 LE, payload [0x5a; 16].
        // An expectation derived from the code under test cannot detect a change to that code.
        // If this literal ever needs updating, the framing changed and every existing WAL is
        // unreadable — fix the encoder, not this array.
        const EXPECTED_DIGEST: [u8; 32] = [
            0xb8, 0xc1, 0xc2, 0x6f, 0x10, 0xd3, 0xd6, 0x36, 0xff, 0x9d, 0x9d, 0x83, 0x61, 0x97,
            0x15, 0x8a, 0x04, 0x82, 0xd1, 0x39, 0x75, 0xcc, 0x1b, 0x7c, 0x52, 0xa3, 0xf1, 0x43,
            0xac, 0x76, 0x45, 0x80,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
    }

    /// Pins the EXACT WIRE BYTES of an ordinary record frame (framing version 1).
    ///
    /// Every WAL on disk is made of these frames, so a change to their layout or to the digest
    /// domain makes every existing WAL unreadable. The digest is a literal computed outside this
    /// crate (`shasum -a 256` over the version byte, seq and fence as u64 LE, and the payload),
    /// for the same reason as in the lineage test above: an expectation derived from
    /// `compute_digest` would move together with it.
    #[test]
    fn record_frame_wire_bytes_are_pinned() {
        let bytes = encode(7, 3, &rec(1)).unwrap();
        let payload: &[u8] = br#"{"type":"turn_finished","step_id":1}"#;

        assert_eq!(bytes.len(), HEADER_LEN + payload.len());
        assert_eq!(&bytes[0..4], &36u32.to_le_bytes(), "payload length");
        assert_eq!(bytes[4], 1, "record framing version");
        assert_eq!(&bytes[5..13], &7u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &3u64.to_le_bytes(), "fence");
        const EXPECTED_DIGEST: [u8; 32] = [
            0xa6, 0xd9, 0xc6, 0xf0, 0x9c, 0xfc, 0x8e, 0x7e, 0x26, 0xaa, 0x04, 0x2d, 0x10, 0xd5,
            0x01, 0x24, 0xd3, 0xde, 0xf4, 0x44, 0x62, 0xe2, 0x91, 0x81, 0x5a, 0x00, 0x37, 0x57,
            0x27, 0x3d, 0x91, 0x1c,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(&bytes[53..], payload, "payload");
    }

    /// Frame `payload` under an arbitrary version byte with a digest that verifies, so a test
    /// can present a reader with a frame this build never writes.
    fn frame_with_version(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> Vec<u8> {
        let digest = compute_digest(ver, seq, fence, payload);
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.push(ver);
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&fence.to_le_bytes());
        out.extend_from_slice(&digest);
        out.extend_from_slice(payload);
        out
    }

    /// A framing version this build does not know is refused by name, never skipped or read.
    ///
    /// Builds that predate gated frames refuse a version-3 frame with exactly this wording,
    /// "unsupported WAL framing version 3", and that refusal is what makes rolling back to
    /// them safe once version-3 frames exist. This build reads version 3, so the property is
    /// asserted for the next unknown version: if a future build writes a new framing version,
    /// rolling back to this build must meet the same named refusal.
    #[test]
    fn an_unknown_framing_version_is_refused_as_unsupported() {
        let bytes = frame_with_version(
            4,
            1,
            1,
            br#"{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}"#,
        );
        match decode_next(&bytes) {
            DecodeStep::Corrupt(error @ FrameError::UnsupportedVersion { ver: 4 }) => {
                assert_eq!(error.to_string(), "unsupported WAL framing version 4");
            }
            _ => panic!("expected an unsupported-version refusal"),
        }
    }

    /// The production `KNOWN_FEATURES` table holds exactly the features this build interprets.
    /// A name enters it only with the release that reads its records, so a build never claims
    /// to understand a frame it would misread; the test-only name is never in it.
    #[test]
    fn the_shipped_known_feature_table_holds_exactly_the_interpreted_features() {
        assert_eq!(
            KNOWN_FEATURES,
            &[
                AGENT_RUN_SCOPE_FEATURE,
                ARCHIVE_INDEX_V2_FEATURE,
                COMPACTION_VIEW_FEATURE,
                COMPACTION_WAIT_FEATURE,
                COMPACTION_FEATURE,
                CONTEXT_CEILING_FEATURE,
                DEFERRED_CALL_FEATURE,
                DELIVERY_ORDER_FEATURE,
                DISPATCH_MODULE_FEATURE,
                FLOW_SCOPE_FEATURE,
                GOOGLE_RPC_ERROR_DETAILS_FEATURE,
                INPUT_TOKEN_CAP_FEATURE,
                OPENROUTER_REASONING_FEATURE,
                PLAN_MANIFEST_FEATURE,
                PROVIDER_TEXT_STATE_FEATURE,
                RESTART_PAUSE_FEATURE,
                RETENTION_FEATURE,
                RUNNER_TAGS_FEATURE,
                RUN_CANCEL_AT_FEATURE,
                SCOPE_FEATURE,
                SESSION_CLASS_FEATURE,
                STEER_QUEUE_FEATURE,
                STEP_TRANSFORMS_FEATURE,
                STRUCTURED_OUTPUT_FEATURE,
                STRUCTURED_OUTPUT_DOWNGRADE_FEATURE,
            ]
        );
        assert_eq!(RUNNER_TAGS_FEATURE, "runner-tags/v1");
        assert_eq!(ARCHIVE_INDEX_V2_FEATURE, "archive-index/v2");
        assert_eq!(COMPACTION_FEATURE, "compaction/v1");
        assert_eq!(COMPACTION_WAIT_FEATURE, "compaction-wait/v1");
        assert_eq!(DEFERRED_CALL_FEATURE, "deferred-call/v1");
        assert_eq!(DISPATCH_MODULE_FEATURE, "dispatch-module/v1");
        assert_eq!(PLAN_MANIFEST_FEATURE, "plan-manifest/v1");
        assert_eq!(PROVIDER_TEXT_STATE_FEATURE, "provider-text-state/v1");
        assert_eq!(OPENROUTER_REASONING_FEATURE, "openrouter-reasoning/v1");
        assert_eq!(RESTART_PAUSE_FEATURE, "restart-pause/v1");
        assert_eq!(SCOPE_FEATURE, "scope/v1");
        assert_eq!(FLOW_SCOPE_FEATURE, "flow-scopes/v1");
        assert_eq!(
            GOOGLE_RPC_ERROR_DETAILS_FEATURE,
            "google-rpc-error-details/v1"
        );
        assert_eq!(INPUT_TOKEN_CAP_FEATURE, "input-token-cap/v1");
        assert_eq!(STEER_QUEUE_FEATURE, "steer-queue/v1");
        assert_eq!(STEP_TRANSFORMS_FEATURE, "step-transforms/v1");
        assert_eq!(STRUCTURED_OUTPUT_FEATURE, "structured-output/v1");
    }

    #[test]
    fn structured_output_downgrade_requires_its_own_feature() {
        let record = WalRecord::StructuredOutputDowngraded {
            run_id: broca_protocol::RunId("r".into()),
        };
        assert_eq!(
            record.required_features(),
            &[STRUCTURED_OUTPUT_DOWNGRADE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
        let old: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != STRUCTURED_OUTPUT_DOWNGRADE_FEATURE)
            .collect();
        assert!(matches!(
            decode_next_with(&bytes, &old),
            DecodeStep::NewerWriter { .. }
        ));
    }

    #[test]
    fn provider_text_state_is_must_understand_before_record_decode() {
        let block = serde_json::json!({"type":"text_with_state","text":"","provider_state":{"format":"gemini","data":"opaque+/="}});
        let message = serde_json::json!({"role":"assistant","content":[block.clone()]});
        let run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}});
        let mut with_input = run.clone();
        with_input["input"] = serde_json::json!([message.clone()]);
        let mut with_policy = run.clone();
        with_policy["config"]["frozen_render_config"] =
            serde_json::json!({"provider_text_state":true});
        let mut refrozen = serde_json::json!({"type":"run_config_refrozen","renderer_version":1,"frozen_render_config":{"provider_text_state":true}});
        let records = [
            with_input,
            with_policy,
            refrozen.clone(),
            serde_json::json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[block]},"usage":{},"retries_used":0}),
            serde_json::json!({"type":"steered","input":[message.clone()]}),
            serde_json::json!({"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[message],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != PROVIDER_TEXT_STATE_FEATURE)
            .collect();
        for value in records {
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(record
                .required_features()
                .contains(&PROVIDER_TEXT_STATE_FEATURE));
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [PROVIDER_TEXT_STATE_FEATURE])
            );
        }
        let legacy: WalRecord = serde_json::from_value(run).unwrap();
        assert!(legacy.required_features().is_empty());
        refrozen["frozen_render_config"]["provider_text_state"] = serde_json::json!(false);
        let legacy: WalRecord = serde_json::from_value(refrozen).unwrap();
        assert!(legacy.required_features().is_empty());
    }

    #[test]
    fn stored_google_rpc_error_details_require_their_wal_feature() {
        let details = serde_json::json!({
            "errorInfo": {"reason": "QUOTA_EXHAUSTED", "metadata": {}},
            "quotaFailure": {"violations": [{"quotaMetric": "model-tier"}]},
            "retryInfo": {"retryDelay": "21m0s"}
        });
        let values = [
            serde_json::json!({
                "type": "run_finished",
                "reason": "error",
                "usage": {},
                "error": {
                    "class": "transient",
                    "message": "429: quota exhausted",
                    "provider_details": details
                }
            }),
            serde_json::json!({
                "type": "run_paused",
                "run_id": "run-1",
                "reason": "auth_required",
                "error": {
                    "class": "transient",
                    "message": "provider error",
                    "provider_details": details
                }
            }),
            serde_json::json!({
                "type": "retracted",
                "submission_id": "submission-1",
                "failure": {
                    "run_id": "run-1",
                    "error": {
                        "class": "transient",
                        "message": "provider error",
                        "provider_details": details
                    }
                }
            }),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != GOOGLE_RPC_ERROR_DETAILS_FEATURE)
            .collect();
        for value in values {
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert_eq!(
                record.required_features(),
                &[GOOGLE_RPC_ERROR_DETAILS_FEATURE]
            );
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            assert!(matches!(
                decode_next_with(&bytes, &older),
                DecodeStep::NewerWriter { missing, .. }
                    if missing == [GOOGLE_RPC_ERROR_DETAILS_FEATURE]
            ));
        }

        let legacy: WalRecord = serde_json::from_value(serde_json::json!({
            "type": "run_finished",
            "reason": "error",
            "usage": {},
            "error": {"class": "transient", "message": "429: quota exhausted"}
        }))
        .unwrap();
        assert!(legacy.required_features().is_empty());
        assert_eq!(encode(1, 1, &legacy).unwrap()[4], FRAMING_VERSION);
    }

    /// Every record kind that carries runner tags, and a run start frozen to render them, is a
    /// gated frame requiring `runner-tags/v1`: a build that predates the feature refuses it by
    /// that name instead of reading the session without its tags. The same records without
    /// tags keep their ungated bytes.
    #[test]
    fn runner_tags_are_must_understand_and_older_readers_refuse_them_by_name() {
        let run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}});
        let result = serde_json::json!({"type":"tool_result","batch_id":"b","result":{"tool_call_id":"c","output":{"kind":"text","text":"r"}},"outcome":"completed"});
        let untagged = [
            run.clone(),
            serde_json::json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[{"type":"text","text":"a"}]},"usage":{},"retries_used":0}),
            result.clone(),
            serde_json::json!({"type":"tool_no_dispatch","batch_id":"b","tool_call_id":"c","reason":{"kind":"denied","reason":"no"}}),
            serde_json::json!({"type":"budget_fired","run_id":"r","kind":"nudge","threshold":1,"ordinal":3}),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != RUNNER_TAGS_FEATURE)
            .collect();
        let mut tagged = Vec::new();
        for value in untagged {
            let plain: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert!(!plain.required_features().contains(&RUNNER_TAGS_FEATURE));
            let mut with_tags = value;
            with_tags["tags"] = if with_tags["type"] == "run_started" {
                serde_json::json!([[7]])
            } else {
                serde_json::json!([7])
            };
            tagged.push(with_tags);
        }
        let mut steered = serde_json::json!({"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"s"}]}]});
        steered["tags"] = serde_json::json!([[8]]);
        tagged.push(steered);
        let mut render = run;
        render["config"]["render_tags"] = serde_json::json!(true);
        tagged.push(render);
        for value in tagged {
            let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert!(
                record.required_features().contains(&RUNNER_TAGS_FEATURE),
                "{value}"
            );
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("a tag-reading build decodes {value}");
            };
            assert_eq!(framed.record, record);
            assert_eq!(
                serde_json::to_value(&framed.record).unwrap()["tags"],
                value["tags"],
                "tags survive the round trip"
            );
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [RUNNER_TAGS_FEATURE]),
                "an older reader refuses {value} by name"
            );
        }
    }

    /// Neither the request fence, an applied view, another answer nor a wait exit can be
    /// skipped by a build that predates the corresponding must-understand feature.
    /// A build that does not know `delivery-order/v1` refuses a delivery-order record as a newer
    /// writer instead of skipping it: skipping it could start a run from a send that may never
    /// start one, or deliver a send that was replaced.
    #[test]
    fn delivery_order_records_refuse_older_readers() {
        let record: WalRecord = serde_json::from_str(
            r#"{"type":"delivery_ordered","submission_id":"sub-r2","order":{"modes":["steer"]}}"#,
        )
        .unwrap();
        let bytes = encode(7, 3, &record).unwrap();
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|known| *known != DELIVERY_ORDER_FEATURE)
            .collect();
        assert!(matches!(decode_next_with(&bytes, &older),
            DecodeStep::NewerWriter { missing, .. } if missing == [DELIVERY_ORDER_FEATURE]));
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
    }

    #[test]
    fn compaction_records_require_their_features_and_refuse_older_readers() {
        for json in include_str!("../tests/fixtures/compaction_records.jsonl").lines() {
            let record: WalRecord = serde_json::from_str(json).unwrap();
            let feature = if json.contains(r#""type":"compaction_wait""#) {
                "compaction-wait/v1"
            } else {
                "compaction/v1"
            };
            assert_eq!(record.required_features(), &[feature]);
            assert!(!record.is_informational());
            let bytes = encode(7, 3, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
                panic!("reader release must decode {json}");
            };
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
            let older: Vec<&str> = KNOWN_FEATURES
                .iter()
                .copied()
                .filter(|known| *known != feature)
                .collect();
            assert!(
                matches!(decode_next_with(&bytes, &older),
                DecodeStep::NewerWriter { missing, .. } if missing == [feature]),
                "{json}"
            );

            // Refusal happens before interpreting a newer writer's closed-set values.
            let payload = format!(
                r#"{{"requires":["{feature}"],"record":{{"type":"compaction_message","kind":"future"}}}}"#
            );
            let future = frame(GATED_FRAMING_VERSION, 7, 3, payload.as_bytes()).unwrap();
            assert!(matches!(decode_next_with(&future, &older),
                DecodeStep::NewerWriter { missing, .. } if missing == [feature]));
        }
    }

    #[test]
    fn agent_run_scope_round_trips_and_old_readers_refuse_its_identity() {
        let payload: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/pre_flow_scoped_run_started.json"
        ))
        .unwrap();
        let mut record: WalRecord = serde_json::from_value(payload["record"].clone()).unwrap();
        if let WalRecord::RunStarted {
            scope: Some(scope), ..
        } = &mut record
        {
            scope.run_id = Some("parent-run".into());
            scope.agent_id = Some("agent-1".into());
            scope.sender = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            });
        } else {
            panic!("fixture must have a scope");
        }
        assert_eq!(
            record.required_features(),
            &[AGENT_RUN_SCOPE_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
            _ => panic!("current reader must preserve agent-run identity"),
        }
        match decode_next_with(&bytes, &[SCOPE_FEATURE, FLOW_SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => {
                assert_eq!(missing, [AGENT_RUN_SCOPE_FEATURE])
            }
            _ => panic!("old reader must refuse agent-run identity"),
        }
    }

    #[test]
    fn flow_scope_round_trips_and_scope_only_readers_refuse_it() {
        let payload: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/pre_flow_scoped_run_started.json"
        ))
        .unwrap();
        let mut record: WalRecord = serde_json::from_value(payload["record"].clone()).unwrap();
        if let WalRecord::RunStarted {
            scope: Some(scope), ..
        } = &mut record
        {
            scope.flow_id = Some("flow-1".into());
            scope.sender = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            });
        } else {
            panic!("fixture must have a scope");
        }
        assert_eq!(
            record.required_features(),
            &[FLOW_SCOPE_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
            _ => panic!("current reader must preserve automation identity"),
        }
        match decode_next_with(&bytes, &[SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => assert_eq!(missing, [FLOW_SCOPE_FEATURE]),
            _ => panic!("scope-only reader must refuse automation identity"),
        }
    }

    #[test]
    fn reader_decodes_restart_pause_v3_and_older_reader_refuses_it() {
        let record: WalRecord = serde_json::from_str(
            r#"{"type":"run_paused","run_id":"r","reason":"restart","usage":{},"retries_used":0}"#,
        )
        .unwrap();
        let bytes = encode_with_requires(7, 1, &[RESTART_PAUSE_FEATURE], &record).unwrap();
        assert_eq!(bytes[4], 3);
        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("reader must decode restart-pause/v1");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert!(
            matches!(decode_next_with(&bytes, &[]), DecodeStep::NewerWriter { missing, .. } if missing == ["restart-pause/v1"])
        );
    }

    #[test]
    fn reader_decodes_frozen_restart_policy_and_historical_absence() {
        let old: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        assert!(matches!(
            old,
            WalRecord::RunStarted {
                on_restart: None,
                ..
            }
        ));
        let mut value: serde_json::Value = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        value["on_restart"] = serde_json::json!("resume");
        let new: WalRecord = serde_json::from_value(value).unwrap();
        assert!(matches!(
            new,
            WalRecord::RunStarted {
                on_restart: Some(broca_protocol::request::OnRestart::Resume),
                ..
            }
        ));
    }

    /// A `Steered` record, as the bytes of its record.
    const STEERED: &str = r#"{"type":"steered","send_id":"steer-1","request_identity_digest":"d","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"about 90 s left, write up now"}]}]}"#;

    /// A `Steered` record is always a gated frame requiring exactly `steer-queue/v1`, even
    /// through plain [`encode`], and this build reads it back intact.
    #[test]
    fn a_steered_record_is_a_gated_frame_requiring_steer_queue_v1_and_round_trips() {
        let record: WalRecord = serde_json::from_str(STEERED).unwrap();
        assert_eq!(record.required_features(), &[STEER_QUEUE_FEATURE]);
        let bytes = encode(5, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        let payload = format!(r#"{{"requires":["steer-queue/v1"],"record":{STEERED}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.record, record);
            }
            _ => panic!("this build must read a steered record"),
        }
    }

    /// A build whose table predates `steer-queue/v1` (the shipped table without it) refuses a
    /// `Steered` frame as a newer writer naming the feature. It never skips the record: that
    /// would drop a user message from the transcript.
    #[test]
    fn a_build_without_steer_queue_v1_refuses_a_steered_record_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(STEERED).unwrap();
        let bytes = encode(6, 1, &record).unwrap();
        match decode_next_with(&bytes, &[SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![STEER_QUEUE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        let _table = lock_feature_table_for_test();
        let _forgotten = forget_feature_for_test(STEER_QUEUE_FEATURE);
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { .. }
        ));
    }

    /// A `ToolDispatchIntent` naming the module its call was dispatched to and the schema it was
    /// dispatched against, as the bytes of its record.
    const DISPATCH_MODULE_INTENT: &str = r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"module_id":"aft","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}}"#;

    /// An intent carrying `module_id` and `schema_pin` is always a gated frame requiring exactly
    /// `dispatch-module/v1`, even through plain [`encode`], and this build reads both back. The
    /// digest is a literal computed outside this crate over the version byte 3, seq 1 and fence
    /// 1 as u64 LE, and the payload.
    #[test]
    fn a_dispatch_module_intent_is_a_gated_frame_requiring_dispatch_module_v1_and_round_trips() {
        let record: WalRecord = serde_json::from_str(DISPATCH_MODULE_INTENT).unwrap();
        assert_eq!(record.required_features(), &[DISPATCH_MODULE_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        let payload =
            format!(r#"{{"requires":["dispatch-module/v1"],"record":{DISPATCH_MODULE_INTENT}}}"#);
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        const EXPECTED_DIGEST: [u8; 32] = [
            0x89, 0x39, 0x61, 0x28, 0xa9, 0xfd, 0x4f, 0xd4, 0xcb, 0x3c, 0x8a, 0x75, 0x87, 0x20,
            0x44, 0xd3, 0xc3, 0xb4, 0xeb, 0xe5, 0xe7, 0xb1, 0xd2, 0x2e, 0x01, 0x09, 0x19, 0x2b,
            0x30, 0x27, 0xac, 0xe3,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("this build must read an intent naming its dispatch module");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert_eq!(encode(1, 1, &framed.record).unwrap(), bytes);
        let WalRecord::ToolDispatchIntent {
            module_id,
            schema_pin,
            ..
        } = &framed.record
        else {
            panic!("expected ToolDispatchIntent");
        };
        assert_eq!(module_id.as_deref(), Some("aft"));
        assert_eq!(
            schema_pin
                .as_ref()
                .map(|p| (p.schema_digest.as_str(), p.semantics)),
            Some(("sha256:5f2c", 2))
        );
    }

    /// Either field alone gates the frame: a module without a pin (a session whose frozen tools
    /// carry no schema digests) and a pin without a module both require `dispatch-module/v1`.
    #[test]
    fn either_dispatch_module_field_alone_gates_the_intent() {
        let module_only = DISPATCH_MODULE_INTENT.replacen(
            r#","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}"#,
            "",
            1,
        );
        let pin_only = DISPATCH_MODULE_INTENT.replacen(r#","module_id":"aft""#, "", 1);
        for json in [module_only, pin_only] {
            assert_ne!(json, DISPATCH_MODULE_INTENT);
            let record: WalRecord = serde_json::from_str(&json).unwrap();
            assert_eq!(record.required_features(), &[DISPATCH_MODULE_FEATURE]);
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            match decode_next(&bytes) {
                DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
                _ => panic!("this build must read {json}"),
            }
        }
    }

    /// A build whose table predates `dispatch-module/v1` (the shipped table without it) refuses
    /// an intent naming its module as a newer writer naming the feature. It never reads the
    /// intent with the module ignored, which would report the call as never dispatched.
    #[test]
    fn a_build_without_dispatch_module_v1_refuses_the_intent_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(DISPATCH_MODULE_INTENT).unwrap();
        let bytes = encode(6, 1, &record).unwrap();
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != DISPATCH_MODULE_FEATURE)
            .collect();
        match decode_next_with(&bytes, &older) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![DISPATCH_MODULE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        let _table = lock_feature_table_for_test();
        let _forgotten = forget_feature_for_test(DISPATCH_MODULE_FEATURE);
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { .. }
        ));
    }

    /// A `Queued` record's `delivery` is ungated: with it the record stays an ordinary frame,
    /// and a record without it keeps the bytes it had before the field existed.
    #[test]
    fn a_queued_delivery_is_an_ungated_field() {
        let plain = r#"{"type":"queued","submission_id":"sub-1","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[]}}"#;
        let record: WalRecord = serde_json::from_str(plain).unwrap();
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], FRAMING_VERSION);
        let WalRecord::Queued { delivery, .. } = &record else {
            panic!("expected Queued");
        };
        assert_eq!(*delivery, None);
        let interrupt = plain.replacen(r#""sub-1","#, r#""sub-1","delivery":"interrupt","#, 1);
        let record: WalRecord = serde_json::from_str(&interrupt).unwrap();
        assert!(record.required_features().is_empty());
        assert_eq!(encode(1, 1, &record).unwrap()[4], FRAMING_VERSION);
        let WalRecord::Queued { delivery, .. } = &record else {
            panic!("expected Queued");
        };
        assert_eq!(*delivery, Some(broca_protocol::Delivery::Interrupt));
    }

    /// A `RunStarted` with a frozen scope, as the bytes of its record.
    const SCOPED_RUN_STARTED: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"principal":{"kind":"reserved","module_id":"prefrontal-core"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3}}"#;

    /// A scoped `RunStarted` is framed as a gated frame requiring exactly `scope/v1`, even
    /// through plain [`encode`], and this build reads it back with the scope intact. The
    /// digest is a literal computed outside this crate over the version byte 3, seq 1 and
    /// fence 1 as u64 LE, and the payload.
    #[test]
    fn a_scoped_run_started_is_a_gated_frame_requiring_scope_v1_and_round_trips() {
        let record: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        assert_eq!(record.required_features(), &[SCOPE_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes, encode_with_requires(1, 1, &[], &record).unwrap());
        assert_eq!(
            bytes,
            encode_with_requires(1, 1, &[SCOPE_FEATURE], &record).unwrap()
        );

        let payload = format!(r#"{{"requires":["scope/v1"],"record":{SCOPED_RUN_STARTED}}}"#);
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        const EXPECTED_DIGEST: [u8; 32] = [
            0xd9, 0x0d, 0x4a, 0xe6, 0xfd, 0x7f, 0x40, 0xa1, 0x68, 0xf7, 0x1b, 0x77, 0xe8, 0xce,
            0xa0, 0xf4, 0x3a, 0xdd, 0x9a, 0x5b, 0x24, 0x66, 0x83, 0x53, 0xe0, 0x3b, 0x77, 0x6b,
            0xa8, 0x4e, 0x67, 0xac,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.record, record);
                let WalRecord::RunStarted { scope, .. } = &framed.record else {
                    panic!("expected RunStarted");
                };
                assert_eq!(
                    scope.as_ref().map(|s| (
                        s.owner.as_owner_string(),
                        s.scope_ref.as_str(),
                        s.scope_epoch
                    )),
                    Some(("reserved:prefrontal-core".to_owned(), "head-1", 3))
                );
            }
            _ => panic!("this build must read a scoped RunStarted"),
        }
    }

    /// A build whose table lacks `scope/v1` (simulated with an empty table) refuses a scoped
    /// `RunStarted` as a newer writer naming the feature, never as corruption, and never by
    /// reading the record with its scope ignored.
    #[test]
    fn a_build_without_scope_v1_refuses_a_scoped_run_started_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        let bytes = encode(4, 2, &record).unwrap();
        match decode_next_with(&bytes, &[]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, 3);
                assert_eq!(missing, vec![SCOPE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
    }

    /// A `RunStarted` carrying a frozen plan manifest, as the bytes of its record: a pretty-printed
    /// plan whose keys are not in sorted order, and three answered provider fetches, one of whose
    /// system text is present but empty.
    fn manifest_run_started_json(run: &str) -> String {
        include_str!("../tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", run)
    }

    /// A `RunStarted` with a plan manifest is framed as a gated frame requiring exactly
    /// `plan-manifest/v1`, even through plain [`encode`], and its bytes survive a decode and
    /// re-encode exactly: the opaque plan keeps its whitespace and key order, and a fetch whose
    /// text is present but empty stays present.
    #[test]
    fn a_manifest_run_started_is_a_gated_frame_requiring_plan_manifest_v1_and_round_trips() {
        use crate::record::FetchOutcome;
        let json = manifest_run_started_json("run-7");
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record.required_features(), &[PLAN_MANIFEST_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        let payload = format!(r#"{{"requires":["plan-manifest/v1"],"record":{json}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("this build must read a RunStarted with a plan manifest");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert_eq!(encode(1, 1, &framed.record).unwrap(), bytes);
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &framed.record
        else {
            panic!("the manifest must come through the decode");
        };
        assert!(manifest
            .plan
            .as_json_text()
            .starts_with("{\n  \"tool_items\": [\n    {\n      \"provider\": \"aft\","));
        assert_eq!(
            manifest.plan_identity,
            "b66c352ab9cbe2c12d1a1c2bceb553c6e7223a846692f945de6bd293be4e20dc"
        );
        let providers: Vec<&str> = manifest
            .fetches
            .iter()
            .map(|f| f.provider.as_str())
            .collect();
        assert_eq!(providers, ["aft", "magic-context", "prefrontal-core"]);
        let FetchOutcome::Answered { tools, text, .. } = &manifest.fetches[0].outcome else {
            panic!("aft answered");
        };
        assert_eq!(tools.len(), 4);
        assert_eq!(text.as_deref(), Some("Read a file before you edit it."));
        let FetchOutcome::Answered { text, .. } = &manifest.fetches[1].outcome else {
            panic!("magic-context answered");
        };
        assert_eq!(text.as_deref(), Some(""));
        assert_eq!(manifest.tools.len(), 8);
        assert_eq!(manifest.tools[0].name, "bash");
        assert_eq!(manifest.join_version, 1);
        assert_eq!(manifest.role_versions["head"], "4");
    }

    /// A record carrying both a frozen scope and a plan manifest requires both features, sorted,
    /// so a build lacking either refuses it.
    #[test]
    fn a_scoped_manifest_run_started_requires_both_features() {
        let json = manifest_run_started_json("run-8").replacen(
            r#","manifest":"#,
            r#","scope":{"owner":{"kind":"direct"},"ref":"head-1","scope_epoch":3},"manifest":"#,
            1,
        );
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(
            record.required_features(),
            &[PLAN_MANIFEST_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        let payload = format!(r#"{{"requires":["plan-manifest/v1","scope/v1"],"record":{json}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);
        match decode_next_with(&bytes, &[SCOPE_FEATURE, STEER_QUEUE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => {
                assert_eq!(missing, vec![PLAN_MANIFEST_FEATURE]);
            }
            _ => panic!("expected NewerWriter"),
        }
    }

    /// A build whose table predates `plan-manifest/v1` (the shipped table without it) refuses a
    /// `RunStarted` carrying a manifest as a newer writer naming the feature: never as
    /// corruption, and never by reading the record with its manifest ignored, which would
    /// render the session without its frozen tools.
    #[test]
    fn a_build_without_plan_manifest_v1_refuses_a_manifest_run_started_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(&manifest_run_started_json("run-9")).unwrap();
        let bytes = encode(4, 2, &record).unwrap();
        match decode_next_with(&bytes, &[SCOPE_FEATURE, STEER_QUEUE_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![PLAN_MANIFEST_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
    }

    /// Test builds read a gated frame naming [`TEST_KNOWN_FEATURE`] through the production
    /// decoder, which is what lets other crates' tests exercise a readable gated frame, while
    /// the shipped table that placement reads still lacks it.
    #[test]
    fn test_builds_know_the_test_only_feature_and_the_shipped_table_does_not() {
        assert!(!KNOWN_FEATURES.contains(&TEST_KNOWN_FEATURE));
        let bytes = encode_with_requires(2, 1, &[TEST_KNOWN_FEATURE], &rec(2)).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, rec(2)),
            _ => panic!("a test build must read the test-only feature"),
        }
    }

    const TEST_FEATURE: &str = "test-feature/v1";

    /// A gated frame's exact bytes, with a digest computed outside this crate over the version
    /// byte 3, seq 1 and fence 1 as u64 LE, and the payload below.
    #[test]
    fn gated_frame_wire_bytes_are_pinned() {
        let bytes = encode_with_requires(1, 1, &[TEST_FEATURE], &rec(1)).unwrap();
        let payload: &[u8] =
            br#"{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}"#;
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        assert_eq!(bytes[4], 3, "gated framing version");
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
        const EXPECTED_DIGEST: [u8; 32] = [
            0xe4, 0x7e, 0x87, 0xcd, 0x99, 0x12, 0x1c, 0x97, 0xa2, 0x49, 0xb8, 0x23, 0x13, 0x1b,
            0x80, 0x8d, 0xdd, 0x18, 0x9d, 0x68, 0xca, 0x27, 0x07, 0x9b, 0x64, 0x1c, 0xcb, 0x4d,
            0x7b, 0xc8, 0x5b, 0x47,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(&bytes[53..], payload, "payload");
    }

    /// With nothing required the encoder writes the ordinary version-1 frame, byte for byte, so
    /// no record that needs no gate becomes unreadable to an older build.
    #[test]
    fn an_empty_requires_list_encodes_an_ordinary_version_1_frame() {
        assert_eq!(
            encode_with_requires(7, 3, &[], &rec(1)).unwrap(),
            encode(7, 3, &rec(1)).unwrap()
        );
        assert_eq!(encode_with_requires(7, 3, &[], &rec(1)).unwrap()[4], 1);
    }

    #[test]
    fn requires_are_sorted_deduplicated_and_held_to_the_capability_grammar() {
        let a = encode_with_requires(1, 1, &["b-two/v1", "a-one/v2", "b-two/v1"], &rec(1));
        let b = encode_with_requires(1, 1, &["a-one/v2", "b-two/v1"], &rec(1));
        assert_eq!(a.unwrap(), b.unwrap());
        for bad in ["", "Upper/v1", "name", "name/v", "name/vx", "/v1", "a_b/v1"] {
            assert!(
                matches!(
                    encode_with_requires(1, 1, &[bad], &rec(1)),
                    Err(FrameError::InvalidFeatureName { .. })
                ),
                "{bad:?} must be refused"
            );
        }
    }

    /// A gated frame whose every required name is in the reader's table decodes to its record.
    /// The table here holds a test-only entry; the shipped table is empty.
    #[test]
    fn a_gated_frame_with_known_features_decodes_to_its_record() {
        let bytes = encode_with_requires(9, 2, &[TEST_FEATURE], &rec(4)).unwrap();
        match decode_next_with(&bytes, &[TEST_FEATURE]) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!((framed.seq, framed.fence), (9, 2));
                assert_eq!(framed.record, rec(4));
            }
            _ => panic!("expected the gated record to decode"),
        }
    }

    /// A gated frame naming a feature the reader lacks is refused as a newer writer, naming
    /// exactly the missing features, never as corruption and never skipped.
    #[test]
    fn an_unknown_feature_is_refused_as_newer_writer_not_corruption() {
        let bytes =
            encode_with_requires(1, 1, &["zz-later/v1", TEST_FEATURE, "aa-newer/v2"], &rec(1))
                .unwrap();
        match decode_next_with(&bytes, &[TEST_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, 3);
                assert_eq!(missing, vec!["aa-newer/v2", "zz-later/v1"]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        // `KNOWN_FEATURES` holds none of the three names this frame requires, so `decode_next`
        // reports all three as missing.
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { ver: 3, missing } if missing.len() == 3
        ));
    }

    /// The requirements are checked before the record is interpreted. A newer build's record
    /// may use a value this build's types reject; it must still read as a newer writer.
    #[test]
    fn requirements_are_checked_before_the_record_is_parsed() {
        let bytes = frame_with_version(
            3,
            1,
            1,
            br#"{"requires":["aa-newer/v1"],"record":{"type":"turn_finished","step_id":"not-a-number"}}"#,
        );
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { ver: 3, .. }
        ));
    }

    /// A digest-valid gated frame whose envelope is malformed is corruption: it cannot have
    /// come from any writer that follows the format.
    #[test]
    fn a_malformed_gated_envelope_is_corruption() {
        for payload in [
            &br#"{"record":{"type":"turn_finished","step_id":1}}"#[..],
            br#"{"requires":["aa-newer/v1"]}"#,
            br#"{"type":"turn_finished","step_id":1}"#,
            b"not json",
        ] {
            assert!(matches!(
                decode_next(&frame_with_version(3, 1, 1, payload)),
                DecodeStep::Corrupt(FrameError::Deserialize(_))
            ));
        }
    }

    /// The digest covers the version byte, so relabelling a gated frame as an ordinary one
    /// fails its digest. Without that, a flipped version byte would turn a record whose
    /// requirements must be checked into one read with no check at all.
    #[test]
    fn a_gated_frame_relabelled_as_version_1_fails_its_digest() {
        let mut bytes = encode_with_requires(5, 1, &[TEST_FEATURE], &rec(1)).unwrap();
        bytes[4] = FRAMING_VERSION;
        assert!(matches!(
            decode_next_with(&bytes, &[TEST_FEATURE]),
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq: 5 })
        ));
    }

    #[test]
    fn truncated_header_or_payload_is_torn_tail() {
        let bytes = encode(1, 1, &rec(1)).unwrap();
        assert!(matches!(decode_next(&bytes[..3]), DecodeStep::TornTail)); // partial header
        assert!(matches!(
            decode_next(&bytes[..HEADER_LEN + 2]),
            DecodeStep::TornTail
        )); // partial payload
    }

    /// Simulate the reader table before the transform reader release, using every hooked
    /// golden and a declarations-only record. Feature refusal must precede payload decoding.
    #[test]
    fn a_build_without_step_transforms_refuses_hooked_wal_as_newer_writer() {
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != STEP_TRANSFORMS_FEATURE)
            .collect();
        for json in [
            include_str!("../tests/golden/step_transforms/run_started.json"),
            include_str!("../tests/golden/step_transforms/queued.json"),
            include_str!("../tests/golden/step_transforms/steered.json"),
            include_str!("../tests/golden/step_transforms/model_step_finished.json"),
            include_str!("../tests/golden/step_transforms/tool_dispatch_intent.json"),
            include_str!("../tests/golden/step_transforms/tool_no_dispatch.json"),
            include_str!("../tests/golden/step_transforms/tool_result.json"),
            include_str!("../tests/golden/step_transforms/tool_result_deny.json"),
        ] {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            let mut record: WalRecord = serde_json::from_value(value["record"].clone()).unwrap();
            for declarations_only in [false, true] {
                if declarations_only {
                    if let WalRecord::RunStarted { hooks, .. } = &mut record {
                        *hooks = None;
                    } else {
                        continue;
                    }
                }
                let bytes = encode(7, 11, &record).unwrap();
                assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
                match decode_next_with(&bytes, &older) {
                    DecodeStep::NewerWriter { ver, missing } => {
                        assert_eq!(ver, GATED_FRAMING_VERSION);
                        assert_eq!(missing, vec![STEP_TRANSFORMS_FEATURE]);
                    }
                    DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
                    _ => panic!("expected newer-writer refusal"),
                }
            }
        }
    }

    #[test]
    fn flipped_payload_byte_is_corruption_not_torn() {
        let mut bytes = encode(1, 1, &rec(1)).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the (complete) payload
        match decode_next(&bytes) {
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq }) => assert_eq!(seq, 1),
            other => panic!(
                "expected DigestMismatch, got {:?}",
                matches!(other, DecodeStep::TornTail)
            ),
        }
    }
}

#[cfg(test)]
mod session_class_tests {
    use super::*;
    #[test]
    fn session_class_is_optional_and_independently_must_understand() {
        let bytes = include_str!("../tests/golden/pre_step_transforms/run_started.json").trim();
        let plain: WalRecord = serde_json::from_str(bytes).unwrap();
        assert!(plain.required_features().is_empty());
        assert_eq!(serde_json::to_string(&plain).unwrap(), bytes);
        let mut value: serde_json::Value = serde_json::from_str(bytes).unwrap();
        value["session_class"] = serde_json::json!("head");
        let classed: WalRecord = serde_json::from_value(value).unwrap();
        assert_eq!(classed.required_features(), &[SESSION_CLASS_FEATURE]);
        assert_eq!(
            serde_json::from_slice::<WalRecord>(&serde_json::to_vec(&classed).unwrap()).unwrap(),
            classed
        );
        let known: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != SESSION_CLASS_FEATURE)
            .collect();
        assert!(
            matches!(decode_next_with(&encode(1, 1, &classed).unwrap(), &known), DecodeStep::NewerWriter { missing, .. } if missing == [SESSION_CLASS_FEATURE])
        );
        assert!(matches!(
            decode_next_with(&encode(1, 1, &plain).unwrap(), &known),
            DecodeStep::Record { .. }
        ));
    }
}
