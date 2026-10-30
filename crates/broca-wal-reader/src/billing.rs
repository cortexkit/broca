//! Billing is a projection of published attempt evidence, never of transcript usage.
//!
//! Consume the entire iterator before saving its checkpoint. In one transaction, persist each
//! unreadable frame and record that its billing evidence could not be determined. A resumed read
//! emits every item after the checkpoint boundary again, including terminals and corrections.
//! Deduplicate by `attempt_id`, and by `correction_id` for corrections: later evidence updates the
//! existing attempt rather than charging it again.
//!
//! After a crash, `writer_opened` emits `PossiblyBilled` for the abandoned reservation. A later
//! recovery `attempt_finished` emits `Finished` with status `PossiblyBilled`. These are two items
//! for one attempt, not two charges.
//!
//! A corrupt header inside the durable range fails closed with `Err(HeaderCorrupt)` and no
//! checkpoint. Subsequent passes stall at the same corruption; operator intervention is required.
//!
//! Checkpoints retain full reservation facts only for open or possibly-billed attempts. Settled
//! attempts retain fixed-size digests and phase to check later corrections and duplicate terminals,
//! plus the record's `copied_in` flag to forward it on later correction items. State grows with the
//! number of attempt IDs, not with the size of historical reservations. Version 1 checkpoints
//! restart from the beginning with `Restart::StateVersion`.

use std::collections::{BTreeMap, VecDeque};
use std::io;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::checkpoint::{self, Boundary, Checkpoint, Restart, KNOWN_FEATURES};
use crate::frame::{self, DecodeStep, Metadata};
use crate::proof::HeadProof;
use crate::resolve::{self, ReadAt};
use crate::unreadable::{Reason, Unreadable};

pub use crate::billing_types::{Envelope, Outcome, Reservation, Sent, Usage, UsageByModel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub lineage_id: [u8; 16],
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BilledKind {
    Finished,
    Warm,
    PossiblyBilled,
    Correction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStatus {
    Completed,
    Failed,
    Cancelled,
    PossiblyBilled,
}

/// The reservation and envelope contain only billing facts, not the enclosing content record.
/// Optional provider fields remain absent; no usage or monetary amount is inferred here.
#[derive(Debug, Clone, PartialEq)]
pub struct Billed {
    pub position: Position,
    pub log_incarnation: String,
    pub attempt_id: String,
    pub kind: BilledKind,
    pub status: AttemptStatus,
    pub copied_in: bool,
    pub reservation: Value,
    pub envelope: Option<Value>,
    pub correction_id: Option<String>,
    pub appended_ms: u64,
}

impl Billed {
    /// Decode the same reservation shape used by the reader's validation, preserving extensions.
    pub fn typed_reservation(&self) -> Result<Reservation, serde_json::Error> {
        serde_json::from_value(self.reservation.clone())
    }

    /// Decode terminal evidence when present, without inferring absent usage fields.
    pub fn typed_envelope(&self) -> Result<Option<Envelope>, serde_json::Error> {
        self.envelope
            .clone()
            .map(serde_json::from_value)
            .transpose()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Billed(Billed),
    Unreadable(Unreadable),
    Restarted { reason: Restart },
}

/// Opening is atomic with respect to feature compatibility: no items or checkpoint are returned
/// if any covered header names an unsupported billing feature, even after readable attempts.
#[derive(Debug)]
pub enum ReadError {
    Unpublished,
    InvalidLineage,
    InvalidPublishedHead,
    NewerWriter { features: Vec<String> },
    HeaderCorrupt { seq: u64, detail: String },
    Io(io::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unpublished => write!(f, "billing log has no valid published head"),
            Self::InvalidLineage => write!(f, "billing log has invalid lineage metadata"),
            Self::InvalidPublishedHead => {
                write!(f, "billing log does not match its published head")
            }
            Self::NewerWriter { features } => write!(
                f,
                "billing log requires unsupported features: {}",
                features.join(", ")
            ),
            Self::HeaderCorrupt { seq, detail } => {
                write!(f, "billing log header at sequence {seq}: {detail}")
            }
            Self::Io(e) => write!(f, "billing log read: {e}"),
        }
    }
}
impl std::error::Error for ReadError {}
impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[must_use = "consume all items and commit evidence before persisting the checkpoint"]
pub struct BilledIter {
    items: VecDeque<Result<Item, ReadError>>,
    checkpoint: Option<Checkpoint>,
    oldest_open_appended_ms: Option<u64>,
    head: HeadProof,
}

impl Iterator for BilledIter {
    type Item = Result<Item, ReadError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.items.pop_front()
    }
}

impl BilledIter {
    /// None until all items have been consumed, or after an untrustworthy header. Payload
    /// evidence counts as consumed only if the caller commits it together with this state.
    pub fn checkpoint(&self) -> Option<&Checkpoint> {
        if self.items.is_empty() {
            self.checkpoint.as_ref()
        } else {
            None
        }
    }

    /// The earliest frame write time among reservations still open at the published head.
    /// Keep a read watermark below this time. Like `checkpoint()`, this returns `None` until
    /// every item is consumed, or after an untrustworthy header. It also returns `None` when
    /// no reservations remain open; possibly-billed reservations are already settled evidence.
    pub fn oldest_open_appended_ms(&self) -> Option<u64> {
        self.checkpoint()?;
        self.oldest_open_appended_ms
    }

    pub fn head(&self) -> &HeadProof {
        &self.head
    }
}

/// Read a live log through an already-open handle. On Windows use crate::open_log so the handle
/// cannot block deletion. A valid resume reads the lineage, one boundary frame, and only the tail.
pub fn billed(
    source: &(impl ReadAt + ?Sized),
    sidecar: &[u8],
    from: Option<&Checkpoint>,
) -> Result<BilledIter, ReadError> {
    let head = HeadProof::decode(sidecar).ok_or(ReadError::Unpublished)?;
    if head.durable_end <= resolve::LINEAGE_LEN || head.durable_seq == 0 {
        return Err(ReadError::InvalidPublishedHead);
    }
    let lineage = resolve::lineage(&resolve::bytes(source, 0, resolve::LINEAGE_LEN)?)
        .ok_or(ReadError::InvalidLineage)?;
    if lineage != head.lineage_id {
        return Err(ReadError::InvalidLineage);
    }
    let mut restart = None;
    let mut checkpoint = None;
    let mut fold = Fold::default();
    if let Some(cp) = from {
        match resolve::validate(source, &head, cp).and_then(|()| Fold::load(&cp.state, cp.seq())) {
            Ok(state) => {
                checkpoint = Some(cp.clone());
                fold = state;
            }
            Err(reason) => restart = Some(reason),
        }
    }
    let mut scan = resolve::scan(
        source,
        &head,
        checkpoint.as_ref().and_then(|c| c.boundary.as_ref()),
    )?;
    if checkpoint.is_some() && scan.corruption.is_some() {
        restart = scan.corruption.as_ref().map(|c| c.reason.clone());
        checkpoint = None;
        fold = Fold::default();
        scan = resolve::scan(source, &head, None)?;
    }
    let start = checkpoint.unwrap_or_else(|| make_checkpoint(lineage, None, &Fold::default()));
    let mut result = project(source, head.clone(), start, fold, scan)?;
    // A restored prefix can retain the checkpoint frame but lose a later terminal that made
    // an earlier reservation safe. Relearn that reservation from the log, not the saved fold.
    if from.is_some() && result.1 {
        restart = Some(Restart::ReservationReopened);
        result = project(
            source,
            head,
            make_checkpoint(lineage, None, &Fold::default()),
            Fold::default(),
            resolve::scan(source, &result.0.head, None)?,
        )?;
    }
    if let Some(reason) = restart {
        result.0.items.push_front(Ok(Item::Restarted { reason }));
    }
    Ok(result.0)
}

/// Resume directly from persisted bytes. A damaged or unsupported checkpoint is a named restart,
/// not an error opening the log and not a partially trusted seed.
pub fn billed_from_checkpoint_bytes(
    source: &(impl ReadAt + ?Sized),
    sidecar: &[u8],
    from: Option<&[u8]>,
) -> Result<BilledIter, ReadError> {
    match from.map(Checkpoint::from_bytes).transpose() {
        Ok(cp) => billed(source, sidecar, cp.as_ref()),
        Err(reason) => {
            let mut read = billed(source, sidecar, None)?;
            read.items.push_front(Ok(Item::Restarted { reason }));
            Ok(read)
        }
    }
}

fn project(
    source: &(impl ReadAt + ?Sized),
    head: HeadProof,
    start: Checkpoint,
    mut fold: Fold,
    scan: resolve::Scan,
) -> Result<(BilledIter, bool), ReadError> {
    if scan
        .corruption
        .as_ref()
        .is_some_and(|c| c.offset >= head.durable_end)
    {
        return Err(ReadError::InvalidPublishedHead);
    }
    let mut items = VecDeque::new();
    let mut previous = start.boundary.clone();
    let mut before = BTreeMap::new();
    for frame in scan.frames {
        let metadata = frame.header.metadata;
        let position = Position {
            lineage_id: head.lineage_id,
            seq: metadata.seq,
        };
        let step = frame::decode_next(&frame.raw, KNOWN_FEATURES).map_err(|e| match e {
            crate::Error::NewerWriter { features } => ReadError::NewerWriter { features },
            other => ReadError::HeaderCorrupt {
                seq: metadata.seq,
                detail: other.to_string(),
            },
        })?;
        let bad = match step {
            DecodeStep::PayloadCorrupt { .. } => Some(Reason::PayloadDigest),
            // An unknown content feature may share a frame with a known billing feature. Emit
            // that frame as Unreadable rather than silently losing its attempt evidence.
            DecodeStep::Skipped { .. }
                if frame.header.requires.iter().any(|f| f.starts_with("bill-")) =>
            {
                Some(Reason::UnsupportedContent)
            }
            DecodeStep::Skipped { .. } => None,
            DecodeStep::Frame(decoded) => match serde_json::from_slice::<Value>(decoded.payload) {
                Err(_) => Some(Reason::PayloadJson),
                Ok(record) => {
                    let is_billing = frame
                        .header
                        .requires
                        .iter()
                        .any(|f| f == "bill-attempts/v1");
                    let reserving = record["type"] == "attempt_reserved";
                    let output = if is_billing && !is_attempt_record(&record) {
                        Err("unrecognized billing record")
                    } else {
                        fold.apply(&record, metadata, head.lineage_id)
                    };
                    match output {
                        Ok(output) => {
                            if reserving {
                                before.insert(
                                    record["attempt_id"].as_str().unwrap().to_owned(),
                                    previous.clone(),
                                );
                            }
                            for id in record["attempt_id"]
                                .as_str()
                                .into_iter()
                                .chain(output.iter().map(|b| b.attempt_id.as_str()))
                            {
                                if fold.attempts.get(id).is_some_and(|a| a.phase() != "open") {
                                    before.remove(id);
                                }
                            }
                            items.extend(output.into_iter().map(|b| Ok(Item::Billed(b))));
                            None
                        }
                        Err(reason) => Some(Reason::BillingRecord(reason.into())),
                    }
                }
            },
            DecodeStep::Incomplete => Some(Reason::Header("incomplete covered frame".into())),
        };
        if let Some(reason) = bad {
            items.push_back(Ok(Item::Unreadable(Unreadable {
                position,
                appended_ms: Some(metadata.appended_ms),
                raw_frame: frame.raw,
                reason,
            })));
        }
        previous = Some(frame.boundary);
    }
    let oldest = fold
        .attempts
        .iter()
        .filter_map(|(id, a)| {
            a.pending()
                .filter(|a| a.phase == "open")
                .map(|a| (id, a.seq))
        })
        .min_by_key(|(_, seq)| *seq);
    let oldest_open_appended_ms = fold
        .attempts
        .values()
        .filter_map(Attempt::pending)
        .filter(|a| a.phase == "open")
        .map(|a| a.appended_ms)
        .min();
    let reopened = oldest.is_some_and(|(_, seq)| seq <= start.seq());
    let mut cp = match oldest.and_then(|(id, _)| before.remove(id)) {
        Some(boundary) => {
            // Undo only the tail's state changes so the saved fold describes the checkpoint,
            // without copying the whole attempt history at each reservation.
            let seq = oldest.expect("an open reservation selected the boundary").1;
            fold.rewind_from(seq);
            Some(make_checkpoint(head.lineage_id, boundary, &fold))
        }
        None if reopened => None,
        None => Some(make_checkpoint(head.lineage_id, previous, &fold)),
    };
    if let Some(corrupt) = scan.corruption {
        items.push_back(Ok(Item::Unreadable(Unreadable {
            position: Position {
                lineage_id: head.lineage_id,
                seq: corrupt.seq,
            },
            appended_ms: None,
            raw_frame: resolve::bytes(
                source,
                corrupt.offset,
                head.durable_end.saturating_sub(corrupt.offset),
            )?,
            reason: Reason::Header(corrupt.detail.clone()),
        })));
        items.push_back(Err(ReadError::HeaderCorrupt {
            seq: corrupt.seq,
            detail: corrupt.detail,
        }));
        cp = None;
    }
    Ok((
        BilledIter {
            items,
            checkpoint: cp,
            oldest_open_appended_ms,
            head,
        },
        reopened,
    ))
}

fn make_checkpoint(lineage: [u8; 16], boundary: Option<Boundary>, fold: &Fold) -> Checkpoint {
    Checkpoint {
        version: checkpoint::VERSION,
        lineage,
        boundary,
        features: checkpoint::feature_digest(),
        state: fold.save(),
    }
}

#[derive(Default)]
struct Fold {
    attempts: BTreeMap<String, Attempt>,
    history: Vec<(u64, String, Option<Attempt>)>,
}

#[derive(Clone)]
enum Attempt {
    Pending(Pending),
    Settled {
        digests: ReservationDigests,
        phase: String,
        copied_in: bool,
    },
}

impl Attempt {
    fn phase(&self) -> &str {
        match self {
            Self::Pending(a) => &a.phase,
            Self::Settled { phase, .. } => phase,
        }
    }

    fn pending(&self) -> Option<&Pending> {
        match self {
            Self::Pending(a) => Some(a),
            Self::Settled { .. } => None,
        }
    }

    fn copied_in(&self) -> bool {
        match self {
            Self::Pending(a) => a.copied_in,
            Self::Settled { copied_in, .. } => *copied_in,
        }
    }

    fn digests(&self) -> ReservationDigests {
        match self {
            Self::Pending(a) => ReservationDigests::new(&a.reservation),
            Self::Settled { digests, .. } => digests.clone(),
        }
    }
}

#[derive(Clone)]
struct ReservationDigests {
    full: [u8; 32],
    without_credential: [u8; 32],
}

impl ReservationDigests {
    fn new(reservation: &Value) -> Self {
        let hash = |v: &Value| {
            let mut canonical = v.clone();
            canonical.sort_all_objects();
            Sha256::digest(serde_json::to_vec(&canonical).expect("JSON value")).into()
        };
        // Credential attribution is the only reservation fact a correction may replace.
        // Keep a second digest so that exception cannot hide changes to other facts.
        let mut without_credential = reservation.clone();
        without_credential
            .as_object_mut()
            .expect("validated reservation")
            .remove("credential");
        Self {
            full: hash(reservation),
            without_credential: hash(&without_credential),
        }
    }
}

#[derive(Clone)]
struct Pending {
    seq: u64,
    writer: [u8; 16],
    appended_ms: u64,
    reservation: Value,
    copied_in: bool,
    phase: String,
}

impl Fold {
    fn replace(&mut self, seq: u64, id: &str, attempt: Attempt) {
        let old = self.attempts.insert(id.into(), attempt);
        self.history.push((seq, id.into(), old));
    }

    fn rewind_from(&mut self, from_seq: u64) {
        while self
            .history
            .last()
            .is_some_and(|(seq, _, _)| *seq >= from_seq)
        {
            let (_, id, previous) = self.history.pop().expect("history has a last entry");
            if let Some(attempt) = previous {
                self.attempts.insert(id, attempt);
            } else {
                self.attempts.remove(&id);
            }
        }
    }

    fn save(&self) -> Value {
        Value::Object(
            self.attempts
                .iter()
                .map(|(id, a)| {
                    (
                        id.clone(),
                        match a {
                            Attempt::Pending(a) => json!({
                                "seq": a.seq, "writer": a.writer, "appended_ms": a.appended_ms,
                                "reservation": a.reservation, "copied_in": a.copied_in, "phase": a.phase
                            }),
                            Attempt::Settled { digests, phase, copied_in } => json!({
                                "phase": phase, "reservation_sha256": hex(&digests.full),
                                "without_credential_sha256": hex(&digests.without_credential),
                                "copied_in": copied_in
                            }),
                        },
                    )
                })
                .collect(),
        )
    }

    fn load(value: &Value, boundary_seq: u64) -> Result<Self, Restart> {
        let mut fold = Self::default();
        for (id, v) in value.as_object().ok_or(Restart::FoldedState)? {
            if !valid_id(id) {
                return Err(Restart::FoldedState);
            }
            let phase = v["phase"].as_str().ok_or(Restart::FoldedState)?;
            if matches!(phase, "terminal" | "released") {
                fold.attempts.insert(
                    id.clone(),
                    Attempt::Settled {
                        digests: ReservationDigests {
                            full: parse_digest(&v["reservation_sha256"])
                                .ok_or(Restart::FoldedState)?,
                            without_credential: parse_digest(&v["without_credential_sha256"])
                                .ok_or(Restart::FoldedState)?,
                        },
                        phase: phase.into(),
                        copied_in: v["copied_in"].as_bool().ok_or(Restart::FoldedState)?,
                    },
                );
                continue;
            }
            if !matches!(phase, "open" | "possible")
                || validate_reservation(&v["reservation"]).is_err()
            {
                return Err(Restart::FoldedState);
            }
            let seq = v["seq"].as_u64().ok_or(Restart::FoldedState)?;
            if seq == 0 || seq > boundary_seq {
                return Err(Restart::FoldedState);
            }
            fold.attempts.insert(
                id.clone(),
                Attempt::Pending(Pending {
                    seq,
                    writer: checkpoint::array(&v["writer"]).ok_or(Restart::FoldedState)?,
                    appended_ms: v["appended_ms"].as_u64().ok_or(Restart::FoldedState)?,
                    reservation: v["reservation"].clone(),
                    copied_in: v["copied_in"].as_bool().ok_or(Restart::FoldedState)?,
                    phase: phase.into(),
                }),
            );
        }
        Ok(fold)
    }

    fn apply(
        &mut self,
        v: &Value,
        m: Metadata,
        lineage: [u8; 16],
    ) -> Result<Vec<Billed>, &'static str> {
        let kind = v["type"].as_str().ok_or("record has no type")?;
        if kind == "writer_opened" || kind == "writer_closed" {
            let writer = checkpoint::array::<16>(&v["writer_id"]).ok_or("invalid writer id")?;
            if writer != m.writer_id {
                return Err("writer payload differs from frame writer");
            }
            if kind == "writer_opened"
                && v["recovered_through_seq"].as_u64() != m.seq.checked_sub(1)
            {
                return Err("writer recovery boundary differs from preceding sequence");
            }
            let mut output = Vec::new();
            for (id, attempt) in &mut self.attempts {
                let Attempt::Pending(a) = attempt else {
                    continue;
                };
                if a.phase == "open"
                    && ((kind == "writer_opened" && a.writer != writer)
                        || (kind == "writer_closed" && a.writer == writer))
                {
                    self.history
                        .push((m.seq, id.clone(), Some(Attempt::Pending(a.clone()))));
                    a.phase = "possible".into();
                    output.push(billed_item(
                        id,
                        a,
                        lineage,
                        BilledKind::PossiblyBilled,
                        None,
                        None,
                        None,
                    ));
                }
            }
            output.sort_by_key(|item| item.position.seq);
            return Ok(output);
        }
        if !matches!(
            kind,
            "attempt_reserved"
                | "attempt_finished"
                | "attempt_released"
                | "attempt_corrected"
                | "model_attempt_finished"
                | "cache_warmed"
        ) {
            return Ok(vec![]);
        }
        // Legacy transcript diagnostics are not attempt evidence and have no deduplication id.
        if matches!(kind, "model_attempt_finished" | "cache_warmed")
            && v.get("attempt_id").is_none()
        {
            if v.get("reservation").is_some() || v.get("envelope").is_some() {
                return Err("billing evidence has no attempt id");
            }
            return Ok(vec![]);
        }
        let id = v["attempt_id"]
            .as_str()
            .filter(|id| valid_id(id))
            .ok_or("invalid attempt id")?;
        let reservation = &v["reservation"];
        validate_reservation(reservation)?;
        let copied_in = v
            .get("copied_in")
            .map_or(Ok(false), |v| v.as_bool().ok_or("invalid copied_in"))?;
        let previous = self.attempts.get(id);
        if kind == "attempt_reserved" {
            if previous.is_some() {
                return Err("attempt id reserved more than once");
            }
            self.replace(
                m.seq,
                id,
                Attempt::Pending(Pending {
                    seq: m.seq,
                    writer: m.writer_id,
                    appended_ms: m.appended_ms,
                    reservation: reservation.clone(),
                    copied_in,
                    phase: "open".into(),
                }),
            );
            return Ok(vec![]);
        }
        let envelope = &v["envelope"];
        validate_envelope(envelope)?;
        let correction = kind == "attempt_corrected";
        let correction_id = if correction {
            Some(
                v["correction_id"]
                    .as_str()
                    .filter(|id| valid_id(id))
                    .ok_or("invalid correction id")?
                    .to_owned(),
            )
        } else {
            None
        };
        let credential_correction = correction && v["reason"] == "credential_attribution_corrected";
        if correction
            && !matches!(
                v["reason"].as_str(),
                Some(
                    "usage_reconciled"
                        | "credential_attribution_corrected"
                        | "provider_evidence_corrected"
                )
            )
        {
            return Err("invalid correction reason");
        }
        if let Some(old) = previous {
            let expected = old.digests();
            let actual = ReservationDigests::new(reservation);
            if if credential_correction {
                expected.without_credential != actual.without_credential
            } else {
                expected.full != actual.full
            } {
                return Err("settlement changes immutable reservation facts");
            }
            if !correction && matches!(old.phase(), "terminal" | "released") {
                return Err("duplicate attempt terminal");
            }
            if correction && old.phase() == "open" {
                return Err("correction precedes settlement");
            }
        }
        if kind == "attempt_released"
            && (envelope["sent"] != false
                || envelope["response_bytes"] != 0
                || [
                    "usage",
                    "usage_by_model",
                    "usage_raw",
                    "cache_write_5m_tokens",
                    "cache_write_1h_tokens",
                ]
                .iter()
                .any(|name| !envelope[name].is_null()))
        {
            return Err("released attempt is not provably unsent");
        }
        if kind == "attempt_finished"
            && (envelope["outcome"] != "unknown"
                || envelope.get("usage").is_some()
                || envelope.get("usage_by_model").is_some())
        {
            return Err("recovery terminal claims measured usage");
        }
        let pending = previous.and_then(Attempt::pending);
        let a = Pending {
            seq: pending.map_or(m.seq, |a| a.seq),
            writer: pending.map_or(m.writer_id, |a| a.writer),
            appended_ms: pending.map_or(m.appended_ms, |a| a.appended_ms),
            reservation: reservation.clone(),
            copied_in: copied_in || previous.is_some_and(Attempt::copied_in),
            phase: if kind == "attempt_released" {
                "released"
            } else {
                "terminal"
            }
            .into(),
        };
        let output = if kind == "attempt_released" {
            vec![]
        } else {
            let billed_kind = if correction {
                BilledKind::Correction
            } else if reservation["warm"] == true {
                BilledKind::Warm
            } else {
                BilledKind::Finished
            };
            vec![billed_item(
                id,
                &a,
                lineage,
                billed_kind,
                Some(envelope.clone()),
                correction_id,
                Some(m),
            )]
        };
        self.replace(
            m.seq,
            id,
            Attempt::Settled {
                digests: ReservationDigests::new(reservation),
                phase: a.phase,
                copied_in: a.copied_in,
            },
        );
        Ok(output)
    }
}

fn billed_item(
    id: &str,
    a: &Pending,
    lineage: [u8; 16],
    kind: BilledKind,
    envelope: Option<Value>,
    correction_id: Option<String>,
    terminal: Option<Metadata>,
) -> Billed {
    let writer = terminal.map_or(a.writer, |m| m.writer_id);
    let status = match envelope.as_ref().and_then(|e| e["outcome"].as_str()) {
        Some("complete") => AttemptStatus::Completed,
        Some("cancelled") => AttemptStatus::Cancelled,
        None | Some("unknown") => AttemptStatus::PossiblyBilled,
        Some(_) => AttemptStatus::Failed,
    };
    Billed {
        position: Position {
            lineage_id: lineage,
            seq: terminal.map_or(a.seq, |m| m.seq),
        },
        log_incarnation: format!("{}.{}", hex(&lineage), hex(&writer)),
        attempt_id: id.into(),
        kind,
        status,
        copied_in: a.copied_in,
        reservation: a.reservation.clone(),
        envelope,
        correction_id,
        appended_ms: terminal.map_or(a.appended_ms, |m| m.appended_ms),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn parse_digest(v: &Value) -> Option<[u8; 32]> {
    let text = v.as_str()?;
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return None;
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(digest)
}
fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn is_attempt_record(v: &Value) -> bool {
    matches!(
        v["type"].as_str(),
        Some(
            "attempt_reserved"
                | "attempt_finished"
                | "attempt_released"
                | "attempt_corrected"
                | "model_attempt_finished"
                | "cache_warmed"
        )
    )
}

fn validate_reservation(v: &Value) -> Result<(), &'static str> {
    for name in [
        "step_id",
        "attempt",
        "send_index",
        "reserved_at_ms",
        "request_body_bytes",
    ] {
        if v[name].as_u64().is_none() {
            return Err("missing integer reservation field");
        }
    }
    for name in ["run_id", "provider", "endpoint_host", "model_sent"] {
        if v[name].as_str().is_none() {
            return Err("missing string reservation field");
        }
    }
    if v["warm"].as_bool().is_none()
        || v["compressed"].as_bool().is_none()
        || !v["credential"].is_object()
        || !v["session"].is_object()
        || !v["run"].is_object()
    {
        return Err("incomplete reservation facts");
    }
    serde_json::from_value::<Reservation>(v.clone()).map_err(|_| "invalid reservation shape")?;
    Ok(())
}

fn validate_envelope(v: &Value) -> Result<(), &'static str> {
    if !(v["sent"].is_boolean() || v["sent"] == "unknown")
        || v["response_bytes"].as_u64().is_none()
        || !matches!(
            v["outcome"].as_str(),
            Some(
                "complete"
                    | "tool_args_trailing_garbage"
                    | "eof_before_completion"
                    | "chunk_error"
                    | "idle_timeout"
                    | "content_timeout"
                    | "byte_limit"
                    | "decode_error"
                    | "rate_floor"
                    | "cancelled"
                    | "sealed_in_flight"
                    | "http_error"
                    | "compression_refused"
                    | "provider_error"
                    | "connect_failed"
                    | "header_timeout"
                    | "no_status"
                    | "unknown"
            )
        )
    {
        return Err("incomplete terminal envelope");
    }
    let present = |name: &str| !v[name].is_null();
    if v["sent"] == false
        && (present("sent_at_ms") || present("http_status") || v["response_bytes"] != 0)
    {
        return Err("unsent attempt claims a response");
    }
    if v["outcome"] == "unknown"
        && [
            "usage",
            "usage_by_model",
            "usage_raw",
            "cache_write_5m_tokens",
            "cache_write_1h_tokens",
        ]
        .iter()
        .any(|name| present(name))
    {
        return Err("unknown outcome claims measured usage");
    }
    validate_usage(v)?;
    let mut reasoning = !v["usage"]["reasoning_tokens"].is_null();
    if !v["usage_by_model"].is_null() {
        let models = v["usage_by_model"]
            .as_array()
            .ok_or("invalid per-model usage")?;
        if models.len() < 2
            || [
                "model_used",
                "usage",
                "cache_write_5m_tokens",
                "cache_write_1h_tokens",
            ]
            .iter()
            .any(|name| present(name))
        {
            return Err("per-model usage conflicts with single-model usage");
        }
        let mut seen = std::collections::BTreeSet::new();
        for model in models {
            let name = model["model_used"]
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or("missing serving model")?;
            if !seen.insert(name) || !model["usage"].is_object() {
                return Err("uncoalesced or missing model usage");
            }
            validate_usage(model)?;
            reasoning |= !model["usage"]["reasoning_tokens"].is_null();
        }
    }
    if reasoning && v["reasoning_included_in_output"].as_bool().is_none() {
        return Err("reasoning usage has no output-inclusion evidence");
    }
    serde_json::from_value::<Envelope>(v.clone()).map_err(|_| "invalid envelope shape")?;
    Ok(())
}

fn validate_usage(v: &Value) -> Result<(), &'static str> {
    if !v["usage"].is_null() && !v["usage"].is_object() {
        return Err("usage is not an object");
    }
    if let Some(usage) = v["usage"].as_object() {
        for name in [
            "input_tokens",
            "cached_input_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
        ] {
            if usage
                .get(name)
                .is_some_and(|v| !v.is_null() && v.as_u64().is_none())
            {
                return Err("usage token count is not an unsigned integer");
            }
        }
    }
    for name in ["cache_write_5m_tokens", "cache_write_1h_tokens"] {
        if !v[name].is_null() && v[name].as_u64().is_none() {
            return Err("invalid cache-write split");
        }
    }
    if let (Some(five), Some(hour)) = (
        v["cache_write_5m_tokens"].as_u64(),
        v["cache_write_1h_tokens"].as_u64(),
    ) {
        if five.checked_add(hour).is_none()
            || five.checked_add(hour) != v["usage"]["cache_write_tokens"].as_u64()
        {
            return Err("cache-write splits differ from total");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::cell::Cell;

    const W: [u8; 16] = [1; 16];
    const NEXT: [u8; 16] = [2; 16];

    #[test]
    fn compression_refused_outcome_has_pinned_serde_name() {
        assert_eq!(
            serde_json::to_string(&Outcome::CompressionRefused).unwrap(),
            r#""compression_refused""#
        );
        assert_eq!(
            serde_json::from_str::<Outcome>(r#""compression_refused""#).unwrap(),
            Outcome::CompressionRefused
        );
        validate_envelope(&serde_json::json!({"sent":true,"response_bytes":0,"outcome":"compression_refused","http_status":415})).unwrap();
    }

    struct Log {
        raw: Vec<u8>,
        frames: Vec<(usize, usize)>,
        head: HeadProof,
    }
    impl Log {
        fn new() -> Self {
            let lineage = [7; 16];
            Self {
                raw: lineage_frame(lineage),
                frames: vec![],
                head: HeadProof {
                    lineage_id: lineage,
                    writer_id: W,
                    durable_seq: 0,
                    durable_end: 69,
                    frame_identity: [0; 32],
                    appended_ms: 0,
                },
            }
        }
        fn push(&mut self, v: Value, writer: [u8; 16]) {
            let feature = if matches!(v["type"].as_str(), Some("writer_opened" | "writer_closed")) {
                "writer-id/v1"
            } else {
                "bill-attempts/v1"
            };
            self.push_bytes(&serde_json::to_vec(&v).unwrap(), writer, &[feature]);
        }
        fn push_bytes(&mut self, payload: &[u8], writer: [u8; 16], features: &[&str]) {
            let seq = self.frames.len() as u64 + 1;
            let mut features = features.to_vec();
            features.push("frame-stamps/v1");
            let raw = frame::encode(
                Metadata {
                    seq,
                    writer_id: writer,
                    fence: 1,
                    appended_ms: seq,
                },
                &features,
                payload,
            )
            .unwrap();
            let h = frame::decode_header(&raw).unwrap().unwrap();
            self.frames.push((self.raw.len(), raw.len()));
            self.raw.extend(raw);
            self.head.writer_id = writer;
            self.head.durable_seq = seq;
            self.head.durable_end = self.raw.len() as u64;
            self.head.frame_identity = h.identity;
            self.head.appended_ms = seq;
        }
        fn open(&mut self, writer: [u8; 16]) {
            self.push(
                json!({"type":"writer_opened", "writer_id":writer,
                "recovered_through_seq": self.frames.len(), "cause":"send"}),
                writer,
            );
        }
        fn read(&self, cp: Option<&Checkpoint>) -> (Vec<Item>, Checkpoint) {
            drain(billed(self.raw.as_slice(), &self.head.encode(), cp).unwrap())
        }
    }

    fn lineage_frame(id: [u8; 16]) -> Vec<u8> {
        let mut raw = 16u32.to_le_bytes().to_vec();
        raw.push(2);
        raw.extend([0; 16]);
        let mut hash = Sha256::new();
        hash.update(&raw[4..]);
        hash.update(id);
        raw.extend(hash.finalize());
        raw.extend(id);
        raw
    }

    fn reservation(warm: bool) -> Value {
        json!({"run_id":"r", "step_id":1, "attempt":0, "send_index":1, "warm":warm,
            "reserved_at_ms":100, "provider":"vendor", "endpoint_host":"example.com",
            "model_sent":"model", "credential":{"auth_method":"apikey", "plan_tier":"future-tier"},
            "session":{"project_root":"/project", "harness":"runner", "session":"s", "session_ref":"ref"},
            "run":{}, "request_body_bytes":123, "compressed":false})
    }
    fn reserved(id: u8, warm: bool) -> Value {
        json!({"type":"attempt_reserved", "attempt_id":format!("{id:032x}"), "reservation":reservation(warm)})
    }
    fn finished(id: u8, warm: bool) -> Value {
        json!({"type":if warm {"cache_warmed"} else {"model_attempt_finished"},
            "attempt_id":format!("{id:032x}"), "reservation":reservation(warm),
            "envelope":{"sent":true, "response_bytes":9, "outcome":"complete", "usage":{"output_tokens":3}}})
    }
    fn close(writer: [u8; 16]) -> Value {
        json!({"type":"writer_closed", "writer_id":writer})
    }
    fn drain(mut iter: BilledIter) -> (Vec<Item>, Checkpoint) {
        let items = iter.by_ref().map(Result::unwrap).collect();
        let cp = iter.checkpoint().unwrap().clone();
        (items, cp)
    }
    fn charges(items: &[Item]) -> Vec<&Billed> {
        items
            .iter()
            .filter_map(|i| {
                if let Item::Billed(b) = i {
                    Some(b)
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn possibly_billed_after_successor_writer() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        assert!(log.read(None).0.is_empty());
        log.open(NEXT);
        let (items, cp) = log.read(None);
        let billed = charges(&items);
        assert_eq!(billed.len(), 1);
        assert_eq!(billed[0].kind, BilledKind::PossiblyBilled);
        assert_eq!(billed[0].status, AttemptStatus::PossiblyBilled);
        assert!(billed[0].envelope.is_none());
        assert_eq!(billed[0].position.seq, 2);
        assert_eq!(billed[0].reservation, reservation(false));
        assert_eq!(cp.seq(), 3);
        assert!(log.read(Some(&cp)).0.is_empty());
        log.push(finished(1, false), NEXT);
        assert_eq!(
            charges(&log.read(Some(&cp)).0)[0].kind,
            BilledKind::Finished
        );
    }

    #[test]
    fn possibly_billed_after_writer_closed() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, true), W);
        log.push(close(W), W);
        let (items, cp) = log.read(None);
        assert_eq!(charges(&items)[0].kind, BilledKind::PossiblyBilled);
        assert_eq!(charges(&items)[0].reservation["warm"], true);
        assert_eq!(cp.seq(), 3);
    }

    #[test]
    fn open_reservation_holds_checkpoint() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(reserved(2, true), W);
        log.push(finished(1, false), W);
        let (items, cp) = log.read(None);
        assert_eq!(charges(&items).len(), 1);
        assert_eq!(
            cp.seq(),
            2,
            "checkpoint is immediately before the oldest still-open warm"
        );
        let bytes = cp.to_bytes();
        let restored = Checkpoint::from_bytes(&bytes).unwrap();
        assert_eq!(restored, cp);
        let (again, again_cp) = log.read(Some(&restored));
        assert_eq!(again, items);
        assert_eq!(again_cp, cp);
        log.push(finished(2, true), W);
        let (items, cp) = log.read(Some(&cp));
        assert_eq!(charges(&items).len(), 2);
        assert_eq!(cp.seq(), 5);
        assert!(log.read(Some(&cp)).0.is_empty());
    }

    #[test]
    fn watermark_stays_below_oldest_open_frame_until_settlement() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(reserved(2, true), W);
        log.push(finished(2, true), W);
        let mut read = billed(log.raw.as_slice(), &log.head.encode(), None).unwrap();
        assert!(read.checkpoint().is_none());
        assert_eq!(read.oldest_open_appended_ms(), None);
        assert!(read.next().unwrap().is_ok());
        assert_eq!(read.oldest_open_appended_ms(), Some(2));
        assert_ne!(
            read.oldest_open_appended_ms(),
            Some(100),
            "not reserved_at_ms"
        );
        let cp = read.checkpoint().unwrap().clone();
        log.push(reserved(3, false), W);
        let mut read = billed(log.raw.as_slice(), &log.head.encode(), Some(&cp)).unwrap();
        read.by_ref().for_each(|item| {
            item.unwrap();
        });
        assert_eq!(read.oldest_open_appended_ms(), Some(2));
        log.push(finished(1, false), W);
        let mut read = billed(log.raw.as_slice(), &log.head.encode(), Some(&cp)).unwrap();
        read.by_ref().for_each(|item| {
            item.unwrap();
        });
        assert_eq!(read.oldest_open_appended_ms(), Some(5));
        let cp = read.checkpoint().unwrap().clone();
        log.push(close(W), W);
        let mut read = billed(log.raw.as_slice(), &log.head.encode(), Some(&cp)).unwrap();
        assert_eq!(read.oldest_open_appended_ms(), None);
        read.by_ref().for_each(|item| {
            item.unwrap();
        });
        assert_eq!(
            read.oldest_open_appended_ms(),
            None,
            "possible is no longer open"
        );
        assert!(read.checkpoint().is_some());
    }

    #[test]
    fn crash_recovery_emits_two_items_for_one_attempt() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.open(NEXT);
        let mut recovery = finished(1, false);
        recovery["type"] = json!("attempt_finished");
        recovery["envelope"] = json!({"sent":"unknown", "response_bytes":0, "outcome":"unknown"});
        log.push(recovery, NEXT);
        let (items, _) = log.read(None);
        let billed = charges(&items);
        assert_eq!(billed.len(), 2);
        assert_eq!(billed[0].attempt_id, billed[1].attempt_id);
        assert_eq!(billed[0].kind, BilledKind::PossiblyBilled);
        assert_eq!(billed[1].kind, BilledKind::Finished);
        assert!(billed
            .iter()
            .all(|b| b.status == AttemptStatus::PossiblyBilled));
        assert_eq!(billed[0].position.seq, 2);
        assert_eq!(billed[1].position.seq, 4);
    }

    #[test]
    fn settled_checkpoint_bytes_are_bounded_per_attempt() {
        let mut fold = Fold::default();
        let mut terminal = finished(1, false);
        terminal["reservation"]["run"]["large_context"] = json!("x".repeat(4096));
        for count in 1..=100_000 {
            terminal["attempt_id"] = json!(format!("{count:032x}"));
            if count % 2 == 0 {
                terminal["type"] = json!("attempt_released");
                terminal["envelope"] =
                    json!({"sent":false, "response_bytes":0, "outcome":"cancelled"});
            } else {
                terminal["type"] = json!("model_attempt_finished");
                terminal["envelope"] = finished(1, false)["envelope"].clone();
            }
            fold.apply(
                &terminal,
                Metadata {
                    seq: count,
                    writer_id: W,
                    fence: 1,
                    appended_ms: count,
                },
                [7; 16],
            )
            .unwrap();
            // Each pass discards rewind history after saving its checkpoint.
            fold.history.clear();
            if [10, 1_000, 100_000].contains(&count) {
                let cp = make_checkpoint([7; 16], None, &fold);
                let size = cp.to_bytes().len();
                println!("{count} settled attempts: {size} checkpoint bytes");
                assert!(size <= count as usize * 280 + 1024, "{count}: {size}");
                assert!(cp.state.as_object().unwrap().values().all(|v| {
                    v.get("reservation").is_none()
                        && matches!(v["phase"].as_str(), Some("terminal" | "released"))
                }));
                let restored = Fold::load(&cp.state, count).unwrap();
                assert_eq!(restored.attempts.len(), count as usize);
                assert!(restored.attempts.values().all(|a| a.pending().is_none()));
            }
        }
    }

    #[test]
    fn compact_settled_state_still_rejects_duplicates_and_immutable_corrections() {
        for released in [false, true] {
            let mut log = Log::new();
            log.open(W);
            let mut terminal = finished(1, false);
            terminal["copied_in"] = json!(true);
            if released {
                terminal["type"] = json!("attempt_released");
                terminal["envelope"] =
                    json!({"sent":false, "response_bytes":0, "outcome":"cancelled"});
            }
            log.push(terminal.clone(), W);
            let (_, cp) = log.read(None);
            let mut cp = Checkpoint::from_bytes(&cp.to_bytes()).unwrap();
            let mut changed = terminal.clone();
            changed["type"] = json!("attempt_corrected");
            changed["correction_id"] = json!("ffeeddccbbaa99887766554433221100");
            changed["reason"] = json!("credential_attribution_corrected");
            changed["reservation"]["credential"]["account_ref"] = json!("new-account");
            changed["reservation"]["model_sent"] = json!("different-model");
            for (record, reason) in [
                (terminal.clone(), "duplicate attempt terminal"),
                (reserved(1, false), "attempt id reserved more than once"),
                (
                    changed.clone(),
                    "settlement changes immutable reservation facts",
                ),
            ] {
                log.push(record, W);
                let (items, next) = log.read(Some(&cp));
                assert!(
                    matches!(&items[..], [Item::Unreadable(e)] if e.reason == Reason::BillingRecord(reason.into()))
                );
                cp = Checkpoint::from_bytes(&next.to_bytes()).unwrap();
            }
            changed["reservation"]["model_sent"] = json!("model");
            changed.as_object_mut().unwrap().remove("copied_in");
            log.push(changed.clone(), W);
            let (items, next) = log.read(Some(&cp));
            assert_eq!(charges(&items).len(), 1);
            assert!(charges(&items)[0].copied_in);
            cp = Checkpoint::from_bytes(&next.to_bytes()).unwrap();
            changed["reason"] = json!("usage_reconciled");
            changed["reservation"]["credential"] = reservation(false)["credential"].clone();
            log.push(changed, W);
            let (items, _) = log.read(Some(&cp));
            assert!(
                matches!(&items[..], [Item::Unreadable(e)] if e.reason == Reason::BillingRecord("settlement changes immutable reservation facts".into()))
            );
        }
    }

    #[test]
    fn version_one_checkpoint_restarts_from_the_beginning_by_name() {
        let mut log = Log::new();
        log.open(W);
        log.push(finished(1, false), W);
        let (expected, cp) = log.read(None);
        let mut old: Value = serde_json::from_slice(&cp.to_bytes()).unwrap();
        old["version"] = json!(1);
        old["state"] = json!({"obsolete":"state"});
        let (items, cp) = drain(
            billed_from_checkpoint_bytes(
                log.raw.as_slice(),
                &log.head.encode(),
                Some(&serde_json::to_vec(&old).unwrap()),
            )
            .unwrap(),
        );
        assert_eq!(
            items[0],
            Item::Restarted {
                reason: Restart::StateVersion
            }
        );
        assert_eq!(items[1..], expected);
        assert_eq!(cp.version, 2);
    }

    #[test]
    fn typed_billing_shapes_match_validated_evidence_and_preserve_extensions() {
        let mut log = Log::new();
        log.open(W);
        let mut terminal = finished(1, false);
        terminal["reservation"]["future_request_fact"] = json!({"nested":true});
        terminal["envelope"]["future_response_fact"] = json!("opaque");
        terminal["envelope"]["usage"]["future_counter"] = json!(7);
        terminal["envelope"]["usage"]["input_tokens"] = json!(0);
        log.push(terminal, W);
        let (items, _) = log.read(None);
        let billed = charges(&items)[0];
        let reservation = billed.typed_reservation().unwrap();
        assert_eq!(reservation.model_sent, "model");
        assert_eq!(
            reservation.extra["future_request_fact"],
            json!({"nested":true})
        );
        let envelope = billed.typed_envelope().unwrap().unwrap();
        assert_eq!(envelope.sent, Sent::Yes);
        assert_eq!(envelope.outcome, Outcome::Complete);
        let usage = envelope.usage.as_ref().unwrap();
        assert_eq!(usage.input_tokens, Some(0));
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(usage.extra["future_counter"], 7);
        assert_eq!(envelope.extra["future_response_fact"], "opaque");
        assert_eq!(
            serde_json::to_value(&envelope).unwrap(),
            billed.envelope.clone().unwrap()
        );
        for (wire, sent) in [
            (json!(true), Sent::Yes),
            (json!(false), Sent::No),
            (json!("unknown"), Sent::Unknown),
        ] {
            assert_eq!(serde_json::from_value::<Sent>(wire.clone()).unwrap(), sent);
            assert_eq!(serde_json::to_value(sent).unwrap(), wire);
        }
        let multi = json!({"sent":true, "response_bytes":9, "outcome":"complete",
            "usage_by_model":[{"model_used":"a", "usage":{"output_tokens":1}},
                {"model_used":"b", "usage":{"cache_write_tokens":3}, "cache_write_5m_tokens":1, "cache_write_1h_tokens":2}]});
        validate_envelope(&multi).unwrap();
        let envelope: Envelope = serde_json::from_value(multi).unwrap();
        let models: Vec<UsageByModel> = envelope.usage_by_model.unwrap();
        assert_eq!(models[1].usage.cache_write_tokens, Some(3));
        assert_eq!(models[1].cache_write_1h_tokens, Some(2));
    }

    #[test]
    fn malformed_typed_optional_facts_are_unreadable() {
        for (section, name, value) in [
            (
                "reservation",
                "input_tokens_estimate",
                json!("not a number"),
            ),
            ("envelope", "http_status", json!(70000)),
            ("envelope", "speed_used", json!(false)),
        ] {
            let mut log = Log::new();
            log.open(W);
            let mut terminal = finished(1, false);
            terminal[section][name] = value;
            log.push(terminal, W);
            let (items, _) = log.read(None);
            assert!(
                matches!(&items[..], [Item::Unreadable(e)] if matches!(e.reason, Reason::BillingRecord(_)))
            );
        }
    }

    #[test]
    fn checkpoint_mismatch_restarts_from_seq_one() {
        let mut older = Log::new();
        older.open(W);
        older.push(reserved(1, false), W);
        older.push(finished(1, false), W);
        let saved_raw = older.raw.clone();
        let saved_head = older.head.clone();
        older.push(reserved(2, true), W);
        older.push(finished(2, true), W);
        let (_, cp) = older.read(None);
        let mut restored = Log::new();
        restored.raw = saved_raw;
        restored.head = saved_head;
        let (items, _) = restored.read(Some(&cp));
        assert_eq!(
            items[0],
            Item::Restarted {
                reason: Restart::BoundaryMissing
            }
        );
        assert_eq!(charges(&items)[0].position.seq, 3);

        let mut replaced = Log::new();
        replaced.head.lineage_id = [8; 16];
        replaced.raw = lineage_frame([8; 16]);
        replaced.open(W);
        replaced.push(reserved(1, false), W);
        replaced.push(finished(1, false), W);
        let (items, _) = replaced.read(Some(&cp));
        assert_eq!(
            items[0],
            Item::Restarted {
                reason: Restart::Lineage
            }
        );
        assert_eq!(charges(&items).len(), 1);

        // Pruning can preserve sequence and byte length while changing the boundary content.
        let mut pruned = Log::new();
        pruned.open(W);
        pruned.push(reserved(1, false), W);
        pruned.push(finished(1, false), W);
        pruned.push(reserved(2, true), W);
        let mut terminal = finished(2, true);
        terminal["envelope"]["usage"]["output_tokens"] = json!(4);
        pruned.push(terminal, W);
        let (items, _) = pruned.read(Some(&cp));
        assert_eq!(
            items[0],
            Item::Restarted {
                reason: Restart::BoundaryHash
            }
        );
        assert_eq!(charges(&items).len(), 2);
    }

    #[test]
    fn payload_corruption_yields_unreadable_and_continues() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(finished(1, false), W);
        log.push(close(W), W);
        let (offset, len) = log.frames[2];
        log.raw[offset + len - 1] ^= 1;
        let (items, cp) = log.read(None);
        let Item::Unreadable(evidence) = &items[0] else {
            panic!("missing evidence")
        };
        assert_eq!(evidence.position.seq, 3);
        assert_eq!(evidence.appended_ms, Some(3));
        assert_eq!(evidence.raw_frame, log.raw[offset..offset + len]);
        assert_eq!(evidence.reason, Reason::PayloadDigest);
        assert_eq!(charges(&items)[0].kind, BilledKind::PossiblyBilled);
        assert_eq!(cp.seq(), 4);
        assert!(log.read(Some(&cp)).0.is_empty());

        let mut json_log = Log::new();
        json_log.open(W);
        json_log.push_bytes(b"{bad json", W, &[]);
        json_log.push(close(W), W);
        let (items, cp) = json_log.read(None);
        assert!(matches!(
            &items[0],
            Item::Unreadable(Unreadable {
                reason: Reason::PayloadJson,
                ..
            })
        ));
        assert_eq!(cp.seq(), 3);
    }

    #[test]
    fn unknown_bill_feature_refuses_log() {
        let mut log = Log::new();
        log.open(W);
        log.push(finished(1, false), W);
        log.push_bytes(b"not JSON", W, &["bill-future/v9", "future-content/v2"]);
        let last = log.raw.len() - 1;
        log.raw[last] ^= 1;
        let result = billed(log.raw.as_slice(), &log.head.encode(), None);
        assert!(
            matches!(result, Err(ReadError::NewerWriter { features }) if features == ["bill-future/v9"])
        );
    }

    #[test]
    fn checkpoint_mismatches_have_named_fallbacks() {
        let mut log = Log::new();
        log.open(W);
        log.push(finished(1, false), W);
        let (_, cp) = log.read(None);
        for (reason, changed) in [
            (Restart::StateVersion, {
                let mut c = cp.clone();
                c.version += 1;
                c
            }),
            (Restart::FeatureTable, {
                let mut c = cp.clone();
                c.features[0] ^= 1;
                c
            }),
            (Restart::BoundaryLength, {
                let mut c = cp.clone();
                c.boundary.as_mut().unwrap().len -= 1;
                c
            }),
            (Restart::FoldedState, {
                let mut c = cp.clone();
                c.state = json!(null);
                c
            }),
        ] {
            let (items, _) = log.read(Some(&changed));
            assert_eq!(items[0], Item::Restarted { reason });
            assert_eq!(charges(&items).len(), 1);
        }
    }

    #[test]
    fn tail_gap_and_invalid_intervening_lineage_restart_with_evidence() {
        for gap in [true, false] {
            let mut log = Log::new();
            log.open(W);
            log.push(finished(1, false), W);
            let (_, cp) = log.read(None);
            if gap {
                log.frames.push((0, 0)); // Model a missing sequence in an otherwise framed tail.
                log.push(close(W), W);
            } else {
                let mut wrong = lineage_frame([8; 16]);
                wrong[30] ^= 1;
                log.raw.extend(wrong);
                log.push(close(W), W);
            }
            let mut read = billed(log.raw.as_slice(), &log.head.encode(), Some(&cp)).unwrap();
            assert_eq!(
                read.next().unwrap().unwrap(),
                Item::Restarted {
                    reason: if gap {
                        Restart::SequenceGap
                    } else {
                        Restart::InterveningLineage
                    }
                }
            );
            assert!(matches!(read.next().unwrap().unwrap(), Item::Billed(_)));
            assert!(matches!(
                read.next().unwrap().unwrap(),
                Item::Unreadable(Unreadable {
                    appended_ms: None,
                    ..
                })
            ));
            assert!(matches!(
                read.next().unwrap(),
                Err(ReadError::HeaderCorrupt { .. })
            ));
            assert!(read.checkpoint().is_none());
            assert_eq!(read.oldest_open_appended_ms(), None);
        }
    }

    struct Counted<T> {
        source: T,
        bytes: Cell<usize>,
    }
    impl<T: ReadAt> ReadAt for Counted<T> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            let n = self.source.read_at(buf, offset)?;
            self.bytes.set(self.bytes.get() + n);
            Ok(n)
        }
    }

    #[test]
    fn valid_resume_reads_only_boundary_and_tail_on_real_files() {
        static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let mut counts = vec![];
        for size in [10, 1000] {
            let mut log = Log::new();
            log.open(W);
            for _ in 0..size {
                log.push_bytes(br#"{"type":"message","body":"private content"}"#, W, &[]);
            }
            log.push(reserved(1, false), W);
            log.push(finished(1, false), W);
            let (_, cp) = log.read(None);
            log.raw.extend(lineage_frame(log.head.lineage_id));
            log.push(reserved(2, true), W);
            log.push(finished(2, true), W);
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/scratch")
                .join(format!(
                    "billing-{}-{}.wal",
                    std::process::id(),
                    SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                ));
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &log.raw).unwrap();
            let source = Counted {
                source: crate::open_log(&path).unwrap(),
                bytes: Cell::new(0),
            };
            let (tail, tail_cp) = drain(billed(&source, &log.head.encode(), Some(&cp)).unwrap());
            counts.push(source.bytes.get());
            let (full, full_cp) = log.read(None);
            let full_tail: Vec<_> = full
                .into_iter()
                .filter(|i| matches!(i, Item::Billed(b) if b.position.seq > cp.seq()))
                .collect();
            assert_eq!(tail, full_tail);
            assert_eq!(tail_cp, full_cp);
            assert_eq!(charges(&tail).len(), 1);
            assert!(!serde_json::to_string(&tail_cp.state)
                .unwrap()
                .contains("private content"));
            std::fs::remove_file(path).unwrap();
        }
        assert_eq!(
            counts[0], counts[1],
            "resume I/O must not grow with the prefix"
        );
    }

    #[test]
    fn every_publication_cut_replays_only_items_after_committed_checkpoint() {
        let mut log = Log::new();
        let mut proofs = vec![];
        log.open(W);
        proofs.push(log.head.encode());
        log.push(reserved(1, false), W);
        proofs.push(log.head.encode());
        log.push(reserved(2, true), W);
        proofs.push(log.head.encode());
        log.push(finished(2, true), W);
        proofs.push(log.head.encode());
        log.open(NEXT);
        proofs.push(log.head.encode());
        log.push(reserved(3, true), NEXT);
        proofs.push(log.head.encode());
        log.push(finished(3, true), NEXT);
        proofs.push(log.head.encode());
        log.push(reserved(4, false), NEXT);
        proofs.push(log.head.encode());
        log.push(close(NEXT), NEXT);
        proofs.push(log.head.encode());
        for crash_at in 0..proofs.len() {
            let mut cp: Option<Checkpoint> = None;
            let mut observed = std::collections::BTreeSet::new();
            for (cut, proof) in proofs.iter().enumerate() {
                let mut read = billed(log.raw.as_slice(), proof, cp.as_ref()).unwrap();
                if cut == crash_at {
                    // The transaction did not commit: neither its rows nor its checkpoint survive.
                    read.next();
                    continue;
                }
                let (items, next_cp) = drain(read);
                for b in charges(&items) {
                    assert!(b.position.seq > cp.as_ref().map_or(0, Checkpoint::seq));
                    observed.insert(b.attempt_id.clone());
                }
                cp = Some(Checkpoint::from_bytes(&next_cp.to_bytes()).unwrap());
            }
            let (items, final_cp) =
                drain(billed(log.raw.as_slice(), &log.head.encode(), cp.as_ref()).unwrap());
            for b in charges(&items) {
                observed.insert(b.attempt_id.clone());
            }
            assert_eq!(observed, (1..=4).map(|id| format!("{id:032x}")).collect());
            assert_eq!(final_cp.seq(), 9);
        }
    }

    #[test]
    fn corrections_preserve_identity_across_checkpoint_resume() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(finished(1, false), W);
        let (_, cp) = log.read(None);
        let mut correction = finished(1, false);
        correction["type"] = json!("attempt_corrected");
        correction["correction_id"] = json!("ffeeddccbbaa99887766554433221100");
        correction["reason"] = json!("provider_evidence_corrected");
        correction["envelope"]["speed_used"] = json!("future-speed");
        log.push(correction.clone(), W);
        let (items, cp) = log.read(Some(&cp));
        assert_eq!(charges(&items)[0].kind, BilledKind::Correction);
        assert_eq!(
            charges(&items)[0].envelope.as_ref().unwrap()["speed_used"],
            "future-speed"
        );
        correction["reservation"]["model_sent"] = json!("different-model");
        log.push(correction.clone(), W);
        let (items, cp) = log.read(Some(&cp));
        assert!(matches!(
            &items[0],
            Item::Unreadable(Unreadable {
                reason: Reason::BillingRecord(_),
                ..
            })
        ));
        correction["reservation"] = reservation(false);
        correction["reservation"]["credential"]["account_ref"] = json!("corrected-account");
        correction["reason"] = json!("credential_attribution_corrected");
        log.push(correction, W);
        assert_eq!(
            charges(&log.read(Some(&cp)).0)[0].reservation["credential"]["account_ref"],
            "corrected-account"
        );
    }

    #[test]
    fn released_and_recovery_terminals_and_failed_warms_keep_their_evidence() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        let mut released = finished(1, false);
        released["type"] = json!("attempt_released");
        released["envelope"] = json!({"sent":false,"response_bytes":0,"outcome":"cancelled"});
        log.push(released, W);
        log.push(reserved(2, true), W);
        let mut failed = finished(2, true);
        failed["envelope"] =
            json!({"sent":true,"response_bytes":0,"outcome":"http_error","http_status":429});
        log.push(failed, W);
        log.push(reserved(3, false), W);
        let mut recovery = finished(3, false);
        recovery["type"] = json!("attempt_finished");
        recovery["envelope"] = json!({"sent":"unknown","response_bytes":0,"outcome":"unknown"});
        log.push(recovery, W);
        let (items, cp) = log.read(None);
        let billed = charges(&items);
        assert_eq!(billed.len(), 2);
        assert_eq!(billed[0].kind, BilledKind::Warm);
        assert_eq!(billed[0].status, AttemptStatus::Failed);
        assert!(billed[0].envelope.as_ref().unwrap().get("usage").is_none());
        assert_eq!(billed[1].status, AttemptStatus::PossiblyBilled);
        assert_eq!(cp.seq(), 7);
        assert!(log.read(Some(&cp)).0.is_empty());
    }

    #[test]
    fn released_attempt_cannot_silently_discard_usage_evidence() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        let mut terminal = finished(1, false);
        terminal["type"] = json!("attempt_released");
        terminal["envelope"] = json!({"sent":false,"response_bytes":0,"outcome":"cancelled","usage_raw":{"provider_tokens":1}});
        log.push(terminal, W);
        log.push(close(W), W);
        let (items, cp) = log.read(None);
        assert!(matches!(
            &items[0],
            Item::Unreadable(Unreadable {
                reason: Reason::BillingRecord(_),
                ..
            })
        ));
        assert_eq!(charges(&items)[0].kind, BilledKind::PossiblyBilled);
        assert_eq!(cp.seq(), 4);
    }

    #[test]
    fn losing_a_post_checkpoint_terminal_relearns_earlier_open_reservations() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(reserved(2, true), W);
        let old_raw = log.raw.clone();
        let old_head = log.head.clone();
        log.push(finished(1, false), W);
        let (_, cp) = log.read(None);
        assert_eq!(cp.seq(), 2);
        log.raw = old_raw;
        log.head = old_head;
        let (items, cp) = log.read(Some(&cp));
        assert_eq!(
            items,
            vec![Item::Restarted {
                reason: Restart::ReservationReopened
            }]
        );
        assert_eq!(cp.seq(), 1);
    }

    #[test]
    fn published_bound_excludes_unpublished_features_and_requires_a_proof() {
        let mut log = Log::new();
        log.open(W);
        log.push(finished(1, false), W);
        let proof = log.head.encode();
        log.push_bytes(b"{}", NEXT, &["bill-unknown/v1"]);
        let (items, cp) = drain(billed(log.raw.as_slice(), &proof, None).unwrap());
        assert_eq!(charges(&items).len(), 1);
        assert_eq!(cp.seq(), 2);
        assert!(matches!(
            billed(log.raw.as_slice(), &[], None),
            Err(ReadError::Unpublished)
        ));
    }

    #[test]
    fn damaged_checkpoint_bytes_restart_and_unknown_billing_records_are_evidence() {
        let mut log = Log::new();
        log.open(W);
        log.push(finished(1, false), W);
        let (items, _) = drain(
            billed_from_checkpoint_bytes(
                log.raw.as_slice(),
                &log.head.encode(),
                Some(b"torn checkpoint"),
            )
            .unwrap(),
        );
        assert_eq!(
            items[0],
            Item::Restarted {
                reason: Restart::FoldedState
            }
        );
        assert_eq!(charges(&items).len(), 1);
        log.push(json!({"type":"unrecognized_attempt_record"}), W);
        let (items, _) = log.read(None);
        assert!(matches!(
            &items[1],
            Item::Unreadable(Unreadable {
                reason: Reason::BillingRecord(_),
                ..
            })
        ));
    }

    #[test]
    fn unsupported_content_on_a_billing_record_is_evidence_not_silence() {
        let mut log = Log::new();
        log.open(W);
        log.push_bytes(
            &serde_json::to_vec(&finished(1, false)).unwrap(),
            W,
            &["bill-attempts/v1", "future-content/v1"],
        );
        let (items, cp) = log.read(None);
        assert!(matches!(
            &items[0],
            Item::Unreadable(Unreadable {
                reason: Reason::UnsupportedContent,
                ..
            })
        ));
        assert_eq!(cp.seq(), 2);
    }

    #[test]
    fn interleaved_warms_and_attempts_survive_independent_crashes() {
        let mut log = Log::new();
        log.open(W);
        log.push(reserved(1, false), W);
        log.push(reserved(2, true), W);
        log.push(finished(2, true), W);
        let mut read = billed(log.raw.as_slice(), &log.head.encode(), None).unwrap();
        assert!(read.checkpoint().is_none());
        assert!(matches!(read.next().unwrap().unwrap(), Item::Billed(_)));
        // A reader crash before committing retains no new checkpoint.
        drop(read);
        let (first, cp) = log.read(None);
        assert_eq!(cp.seq(), 1);
        assert_eq!(charges(&first)[0].attempt_id, format!("{:032x}", 2));
        // A writer crash publishes no close record; a later writer_opened record lets the
        // fold identify its unfinished attempts as possibly billed.
        assert_eq!(log.read(Some(&cp)).0, first);
        log.open(NEXT);
        log.push(reserved(3, true), NEXT);
        let (second, cp) = log.read(Some(&cp));
        assert_eq!(
            charges(&second)
                .iter()
                .map(|b| (&b.attempt_id, b.kind))
                .collect::<Vec<_>>(),
            vec![
                (&format!("{:032x}", 2), BilledKind::Warm),
                (&format!("{:032x}", 1), BilledKind::PossiblyBilled)
            ]
        );
        assert_eq!(cp.seq(), 5);
        log.push(finished(3, true), NEXT);
        log.push(reserved(4, false), NEXT);
        log.push(close(NEXT), NEXT);
        let (third, cp) = log.read(Some(&cp));
        assert_eq!(
            charges(&third)
                .iter()
                .map(|b| b.attempt_id.clone())
                .collect::<Vec<_>>(),
            vec![format!("{:032x}", 3), format!("{:032x}", 4)]
        );
        assert_eq!(cp.seq(), 9);
        assert!(log.read(Some(&cp)).0.is_empty());
    }
}
