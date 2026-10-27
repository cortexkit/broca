//! CONTROL lane payload, runner → caller — the OutputStream control units. Sent
//! on the reliable, reserved-capacity priority lane (a display flood can't starve
//! it).
//!
//! This is the caller-facing surface. It overlaps the provider→runner control
//! schema but adds the run-level events the runner owns (`RunStarted`/
//! `RunFinished`, `ApprovalRequest`, runner-produced `ToolResult`).

use std::ops::Deref;
use std::sync::Arc;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    approval::ApprovalRequest,
    content::{AssistantMessage, FinishReason, ToolCall, ToolResult},
    error::{OpenErrorClass, ProviderError},
    ids::{RunId, StepId, SubmissionId, ToolCallId},
    request::BudgetKind,
    usage::Usage,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlUnit {
    RunStarted {
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        submission_id: Option<SubmissionId>,
    },
    StepStarted {
        step_id: StepId,
    },
    /// The run remains active while a compaction view can still arrive before this deadline.
    ContextCeilingWait {
        run_id: RunId,
        reason: String,
        deadline_ms: u64,
    },
    /// The run remains active while a rewrite ride's answer, pending for over a second, can
    /// still arrive before this deadline (llm-runner 0.5.2 §6 `compaction_wait`). A
    /// runner-owned wait: a reader recognizes it by `deadline_ms`, never by `reason`.
    CompactionWait {
        run_id: RunId,
        reason: String,
        deadline_ms: u64,
    },
    /// A caller-supplied budget message was durably injected into the prompt.
    BudgetFired {
        run_id: RunId,
        kind: BudgetKind,
        threshold: u64,
        ordinal: u64,
    },
    /// Validated/persisted assembled assistant message.
    AssistantMessage {
        message: Arc<AssistantMessage>,
    },
    /// A tool call the runner is about to dispatch (extracted from the assembled
    /// message, post-validation).
    ToolCall {
        call: ToolCall,
    },
    /// A tool result (runner-produced via subc→AFT, or a denied/error result).
    ToolResult {
        result: ToolResult,
    },
    /// One model step completed.
    ///
    /// `usage` is THIS STEP's usage, never a running total: a consumer wanting a run's spend
    /// sums it across every `StepFinished`. Summing is correct for spend because each step is a
    /// separately billed call, and wrong for context occupancy because each step re-sends the
    /// growing prompt -- so the summed input counts the same prompt repeatedly and can exceed
    /// any window the model ever held. For occupancy, use the LAST step's `fill_tokens()`.
    StepFinished {
        step_id: StepId,
        usage: Usage,
        #[serde(deserialize_with = "crate::content::deserialize_caller_finish_reason")]
        finish_reason: FinishReason,
    },
    /// Approval round-trip request; the caller replies on the control back-channel.
    ApprovalRequest {
        request: ApprovalRequest,
    },
    /// Terminal typed error.
    Error {
        error: SharedProviderError,
    },
    /// Terminal-for-this-attach PAUSE: the run is paused (not finished) awaiting a
    /// resolvable cause (re-auth). A subscriber from `start` sees this as the final
    /// projected event for a paused run, mirroring how `RunFinished` is terminal — but
    /// the run is RESUMABLE under the same run_id once the cause clears.
    Paused {
        run_id: RunId,
        reason: PauseReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SharedProviderError>,
    },
    RunFinished {
        /// Which run this terminates.
        ///
        /// `RunStarted`, `Paused` and `BudgetFired` all carry the run they concern; this
        /// one did not, so a subscriber attaching mid-stream could not attribute a
        /// terminal to a run. That is not hypothetical: a resumed run writes NO new
        /// `RunStarted` (same episode), and a Live attach excludes pre-attach history, so
        /// a client that identifies its run by waiting for `RunStarted` waits forever
        /// through its own successful completion.
        ///
        /// `None` means the payload came from a producer before this field existed, NOT
        /// that the terminal has no run. Consumers must fall back rather than assume.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<RunId>,
        reason: RunFinishReason,
        /// Absent if the run ended before a model step completed. A completed step that reports
        /// zero tokens is present as `Some(Usage::default())`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
        /// The provider finish reason recorded on the last model step in this run.
        /// Absent when the run produced no model step or the WAL predates this metadata.
        #[serde(
            default,
            deserialize_with = "crate::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Tool calls whose side effects may have happened without a durable definite result.
        /// Present only for interrupted terminals; an empty vector proves no call was dispatched.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        indeterminate_tool_calls: Option<Vec<ToolCallId>>,
        #[serde(default)]
        retries_used: u32,
    },
    /// Stored-message identity for a lifecycle event needing its own control unit.
    /// The stream assigns a durable cursor without repeating the message body.
    MessageBoundary {
        mid: String,
    },
    /// Tell the caller to discard partial display state (paired with a display-lane
    /// reset) — e.g. when a stream generation is abandoned.
    /// Unknown future control events use this conservative reset fallback.
    #[serde(other)]
    DisplayReset,
}

/// A provider error on a caller-facing surface.
///
/// Serialization maps the runner-local `ProviderUnavailable` class to the shared `transient`
/// class. The owned wrapper prevents a caller-facing schema from accidentally storing a raw
/// [`ProviderError`], while durable WAL records continue to use that raw type. On a caller read,
/// an unfamiliar class stays available through `class_wire`; `as_inner` uses `permanent` only as
/// a non-retrying compatibility fallback.
#[derive(Debug, Clone, PartialEq)]
pub struct SharedProviderError {
    inner: Box<ProviderError>,
    wire_class: Option<OpenErrorClass>,
}

impl SharedProviderError {
    /// Borrow the provider error retained after deserializing a caller-facing response.
    ///
    /// If a newer caller class is unknown to this process, `inner.class` is the conservative
    /// `permanent` fallback. Use [`Self::class_wire`] to inspect the original open string.
    pub fn as_inner(&self) -> &ProviderError {
        &self.inner
    }

    /// Recover the provider error for non-caller-facing processing.
    pub fn into_inner(self) -> ProviderError {
        *self.inner
    }

    /// The caller-facing class spelling, including an unfamiliar value from a newer producer.
    pub fn class_wire(&self) -> &str {
        if let Some(class) = &self.wire_class {
            return class.as_str();
        }
        match self.inner.class.shared_class() {
            crate::ErrorClass::Transient => "transient",
            crate::ErrorClass::Permanent => "permanent",
            crate::ErrorClass::Auth => "auth_required",
            crate::ErrorClass::ContextOverflow => "context_overflow",
            crate::ErrorClass::ProviderUnavailable => {
                unreachable!("shared_class maps local classes")
            }
        }
    }

    /// Whether the producer sent a class outside this build's pinned shared vocabulary.
    pub fn has_unknown_class(&self) -> bool {
        self.wire_class.is_some()
    }
}

impl<'de> Deserialize<'de> for SharedProviderError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let raw_class = value
            .get("class")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| D::Error::custom("caller provider error is missing string class"))?;
        let known = serde_json::from_value::<crate::ErrorClass>(serde_json::Value::String(
            raw_class.clone(),
        ))
        .is_ok();
        if known {
            let inner = serde_json::from_value(value).map_err(D::Error::custom)?;
            return Ok(Self {
                inner: Box::new(inner),
                wire_class: None,
            });
        }

        let Some(object) = value.as_object_mut() else {
            return Err(D::Error::custom("caller provider error must be an object"));
        };
        object.insert(
            "class".into(),
            serde_json::Value::String("permanent".into()),
        );
        let inner = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self {
            inner: Box::new(inner),
            wire_class: Some(OpenErrorClass::from_wire(raw_class)),
        })
    }
}

impl Deref for SharedProviderError {
    type Target = ProviderError;

    fn deref(&self) -> &Self::Target {
        self.as_inner()
    }
}

impl From<ProviderError> for SharedProviderError {
    fn from(error: ProviderError) -> Self {
        Self {
            inner: Box::new(error),
            wire_class: None,
        }
    }
}

impl From<SharedProviderError> for ProviderError {
    fn from(error: SharedProviderError) -> Self {
        *error.inner
    }
}

impl Serialize for SharedProviderError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(class) = &self.wire_class {
            let mut value = serde_json::to_value(&self.inner).map_err(serde::ser::Error::custom)?;
            let Some(object) = value.as_object_mut() else {
                return Err(serde::ser::Error::custom(
                    "provider error did not serialize as an object",
                ));
            };
            object.insert(
                "class".into(),
                serde_json::Value::String(class.as_str().to_owned()),
            );
            value.serialize(serializer)
        } else {
            let shared_class = self.inner.class.shared_class();
            if shared_class == self.inner.class {
                self.inner.serialize(serializer)
            } else {
                let mut error = self.inner.clone();
                error.class = shared_class;
                error.serialize(serializer)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunFinishReason {
    /// `stop_when` satisfied / no more tool calls.
    Completed,
    /// Structural max-steps terminal outcome.
    MaxSteps,
    /// The session's hard input-token cap was reached, or input usage was unreported.
    BudgetExhausted,
    /// The runner declined the next request before sending it, without a provider failure.
    ContextCeiling,
    /// A caller asked for this run to stop, via `run.cancel`. Distinct from `Interrupted`
    /// so the durable record separates "someone decided" from "something died" -- a
    /// distinction an audit surface cannot recover after the fact.
    ///
    /// RESERVED FOR A CALLER'S DECISION. An automatic seal (a policy sweep retiring a
    /// long-dead pause, say) must mint its own reason rather than borrow this one, or it
    /// launders a policy action as a human one and the audit trail silently stops meaning
    /// what it says.
    Cancelled,
    /// A tool dispatch returned an AMBIGUOUS outcome (the side effect MAY have
    /// applied). The run stops for human/doctor review WITHOUT feeding the unknown
    /// outcome back into a model step — re-running it could duplicate the effect.
    Interrupted,
    /// The loop failed: a permanent provider error, or a failure the retry budget could not
    /// absorb. This reason only says the run ended because something went wrong rather than
    /// because it finished or was stopped.
    ///
    /// WHERE THE TYPED ERROR TRAVELS, per surface. This doc used to say it "rides on
    /// `RunFinished.error`" -- true of `WalRecord::RunFinished` (broca-wal), and FALSE of
    /// [`ControlUnit::RunFinished`] in this file, which has no `error` field. Two types, one
    /// variant name, opposite answers, and this is the file a consumer reads to learn the
    /// wire: prefrontal wrote a branch reading `error` off a `run_finished` stream unit that
    /// could never fire (iceteaSA, issue #21). A sentence that is true about a type the
    /// reader cannot reach is worse than no sentence.
    ///
    /// - `session.subscribe`: a SEPARATE [`ControlUnit::Error`] unit carries the typed error,
    ///   emitted BEFORE the [`ControlUnit::RunFinished`] terminal. A consumer must correlate
    ///   the two; the terminal alone says only that the run ended badly.
    /// - `session.read`: `lineage_state.error`, alongside `lineage_state.state`.
    /// - `run.status`: see [`RunFinishReason`]'s note on the status surface -- the terminal
    ///   is reported and the cause is not yet.
    /// - the durable WAL: `WalRecord::RunFinished.error`, which is the authority all of the
    ///   above are projected from.
    Error,
    /// The prompt-reduction transform module was unreachable (after its own retry +
    /// last-known-good fallback), so the turn could not be rendered. Distinct from `Error`
    /// (a provider/loop failure) so the doctor/status surface can tell "transform down" from
    /// a provider error. The run keeps the untransformed prompt durable, so it resumes once
    /// the transform is reachable again.
    TransformUnavailable,
}

impl RunFinishReason {
    /// The spelling the WAL and every caller-facing surface use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::MaxSteps => "max_steps",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ContextCeiling => "context_ceiling",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::Error => "error",
            Self::TransformUnavailable => "transform_unavailable",
        }
    }
}

/// Why a run is PAUSED (non-terminal, resumable) rather than finished. The shared
/// vocabulary for a resumable pause — used both on the caller-facing control surface
/// (`ControlUnit::Paused`) and in the durable WAL (`WalRecord::RunPaused`), so the two
/// can't drift. Both credential refresh and graceful restart preserve the episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    /// The provider needs (re-)authentication — the credential expired/was revoked
    /// mid-run. Resumable: after re-auth the SAME run_id continues.
    AuthRequired,
    /// A graceful process stop at a clean boundary; the same episode can resume after boot.
    Restart,
    /// The run waits for the late result of a tool call its provider answered with a `held`
    /// marker (tool-provider contract §4.1, `held-reply/v1`). The caller does not resume it:
    /// the intended design is that broca resumes the same run itself when the provider
    /// publishes the call's late result. The reason is informational; which calls the run
    /// waits on is reported separately.
    ///
    /// In the WAL this value is must-understand (feature `provider-held/v1`): a build without
    /// that feature refuses the frame instead of misreading the pause. No production path
    /// writes it yet, and this build's WAL reader does not list the feature.
    ToolHeld,
}

impl PauseReason {
    /// The spelling the WAL and the control stream use.
    pub fn as_str(self) -> &'static str {
        match self {
            PauseReason::AuthRequired => "auth_required",
            PauseReason::Restart => "restart",
            PauseReason::ToolHeld => "tool_held",
        }
    }
}

/// A run state value as a caller reads it, where a newer runner may send a value this build
/// does not know.
///
/// Runner state vocabularies, such as why a run finished or why it paused, grow over time. A
/// client that decoded them as closed enums would fail to read any reply that carries a new
/// value, so caller-facing replies decode into this type instead. A value this build knows
/// becomes [`OpenState::Known`]; any other string is kept verbatim as [`OpenState::Unknown`].
///
/// An unknown value is never mapped onto the nearest known one. It does not mean the run
/// failed, and it does not mean the run needs re-authentication: the caller only knows that the
/// run reached a state this build cannot interpret, and it must handle that as its own case.
///
/// A known value is written exactly as the closed enum writes it, and an unknown one as its
/// received string, so for the run finish and pause reasons both are bare strings. Durable WAL records keep the closed enums and still refuse a value
/// they do not know.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpenState<T> {
    /// A value from this build's vocabulary.
    Known(T),
    /// A value from a newer vocabulary, exactly as the producer spelled it.
    Unknown(String),
}

/// Why a run finished, as a caller reads it: see [`OpenState`].
pub type OpenRunFinishReason = OpenState<RunFinishReason>;

/// Why a run paused, as a caller reads it: see [`OpenState`].
pub type OpenPauseReason = OpenState<PauseReason>;

impl<T> OpenState<T> {
    /// The value when this build knows it.
    pub fn known(&self) -> Option<&T> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }
}

impl<T> From<T> for OpenState<T> {
    fn from(value: T) -> Self {
        Self::Known(value)
    }
}

impl OpenState<RunFinishReason> {
    /// The wire spelling, including an unknown value as it was received.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Known(reason) => reason.as_str(),
            Self::Unknown(raw) => raw,
        }
    }
}

impl OpenState<PauseReason> {
    /// The wire spelling, including an unknown value as it was received.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Known(reason) => reason.as_str(),
            Self::Unknown(raw) => raw,
        }
    }
}

impl<T: Serialize> Serialize for OpenState<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Known(value) => value.serialize(serializer),
            Self::Unknown(raw) => serializer.serialize_str(raw),
        }
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for OpenState<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Only an unfamiliar string is open. A value of the wrong JSON type is still refused,
        // since that is a malformed reply rather than a newer vocabulary.
        let raw = String::deserialize(deserializer)?;
        let known =
            T::deserialize(serde::de::value::StrDeserializer::<serde::de::value::Error>::new(&raw));
        Ok(match known {
            Ok(value) => Self::Known(value),
            Err(_) => Self::Unknown(raw),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorClass;

    fn unavailable_error() -> ProviderError {
        ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::ProviderUnavailable,
            message: "provider stream disconnected".into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }
    }

    #[test]
    fn run_started_submission_id_is_wire_optional() {
        let legacy: ControlUnit = serde_json::from_value(serde_json::json!({
            "type": "run_started",
            "run_id": "run-1"
        }))
        .unwrap();
        assert_eq!(
            legacy,
            ControlUnit::RunStarted {
                run_id: RunId("run-1".into()),
                submission_id: None,
            }
        );

        let queued = ControlUnit::RunStarted {
            run_id: RunId("run-2".into()),
            submission_id: Some(SubmissionId("sub-run-2".into())),
        };
        assert_eq!(
            serde_json::to_value(queued).unwrap(),
            serde_json::json!({
                "type": "run_started",
                "run_id": "run-2",
                "submission_id": "sub-run-2"
            })
        );
    }

    #[test]
    fn control_error_serializes_provider_unavailable_as_shared_transient() {
        let error_wire = serde_json::to_value(ControlUnit::Error {
            error: unavailable_error().into(),
        })
        .unwrap();

        assert_eq!(error_wire["error"]["class"], "transient");
    }

    #[test]
    fn control_pause_serializes_provider_unavailable_as_shared_transient() {
        let paused_wire = serde_json::to_value(ControlUnit::Paused {
            run_id: RunId("run-1".into()),
            reason: PauseReason::AuthRequired,
            error: Some(unavailable_error().into()),
        })
        .unwrap();

        assert_eq!(paused_wire["error"]["class"], "transient");
    }

    #[test]
    fn shared_provider_error_preserves_public_class_json_bytes() {
        for class in [
            ErrorClass::Transient,
            ErrorClass::Permanent,
            ErrorClass::Auth,
            ErrorClass::ContextOverflow,
        ] {
            for raw in [
                ProviderError {
                    provider_details: None,
                    provider_subcode: None,
                    refusal: None,
                    class,
                    message: "same JSON shape".into(),
                    status: Some(503),
                    retry_after_secs: Some(1.5),
                    provider_code: Some("provider_code".into()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                },
                ProviderError {
                    provider_details: None,
                    provider_subcode: None,
                    refusal: None,
                    class,
                    message: "optional fields omitted".into(),
                    status: None,
                    retry_after_secs: None,
                    provider_code: None,
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                },
            ] {
                assert_eq!(
                    serde_json::to_string(&SharedProviderError::from(raw.clone())).unwrap(),
                    serde_json::to_string(&raw).unwrap(),
                    "{class:?} keeps its established caller JSON"
                );
            }
        }
    }

    #[test]
    fn the_run_finished_terminal_carries_no_typed_error_and_the_doc_says_where_it_does() {
        // A TRIPWIRE FOR A DOC, not for the wire.
        //
        // `RunFinishReason::Error` used to say the typed error "rides on RunFinished.error".
        // True of `WalRecord::RunFinished` in broca-wal; false of the ControlUnit in THIS
        // file, which is the one a consumer reads to learn the wire. prefrontal wrote a
        // branch reading `error` off a run_finished stream unit that could never fire
        // (iceteaSA, issue #21).
        //
        // The doc now names the surface for each answer. Nothing made the old sentence wrong
        // in a way anything could catch, and the CHANGE UNDER DISCUSSION -- adding `error` to
        // this terminal so a consumer need not correlate two units -- would invert the new
        // sentence just as silently, in the opposite direction.
        //
        // So this asserts the shape the doc describes, and fails by naming the doc.
        let terminal = ControlUnit::RunFinished {
            run_id: Some(RunId("run-x".into())),
            reason: RunFinishReason::Error,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            retries_used: 0,
        };
        let json = serde_json::to_value(&terminal).expect("terminal serializes");
        assert!(
            json.get("error").is_none(),
            "ControlUnit::RunFinished now carries `error`. That may be the right change -- if \
             you made it, UPDATE THE `RunFinishReason::Error` DOC, which currently tells \
             consumers the cause arrives on a separate ControlUnit::Error unit and must be \
             correlated. A consumer that believes the old sentence writes a branch that \
             cannot fire; one that believes this sentence after the field lands correlates \
             two units for no reason. got: {json}"
        );
    }

    #[test]
    fn run_finished_indeterminate_tool_calls_are_additive() {
        #[derive(Debug, Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum LegacyControlUnit {
            RunFinished {
                reason: RunFinishReason,
                #[serde(default)]
                usage: Option<Usage>,
                #[serde(default)]
                final_step_finish_reason: Option<FinishReason>,
                #[serde(default)]
                retries_used: u32,
            },
        }

        let terminal = ControlUnit::RunFinished {
            run_id: Some(RunId("run-1".into())),
            reason: RunFinishReason::Interrupted,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: Some(vec![ToolCallId("call-1".into())]),
            retries_used: 2,
        };
        let wire = serde_json::to_value(&terminal).unwrap();
        assert_eq!(
            wire["indeterminate_tool_calls"],
            serde_json::json!(["call-1"])
        );

        let LegacyControlUnit::RunFinished {
            reason,
            usage,
            final_step_finish_reason,
            retries_used,
        } = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(reason, RunFinishReason::Interrupted);
        assert_eq!(usage, None);
        assert_eq!(final_step_finish_reason, None);
        assert_eq!(retries_used, 2);

        // The run must ride out on the wire, and must decode back as absent when a
        // producer predating the field omits it. Same two directions the
        // `indeterminate_tool_calls` addition pinned, for the same reason.
        assert_eq!(wire["run_id"], serde_json::json!("run-1"));
        let mut without_run = wire.clone();
        without_run.as_object_mut().unwrap().remove("run_id");
        assert!(matches!(
            serde_json::from_value::<ControlUnit>(without_run).unwrap(),
            ControlUnit::RunFinished { run_id: None, .. }
        ));

        let mut legacy_wire = wire;
        legacy_wire
            .as_object_mut()
            .unwrap()
            .remove("indeterminate_tool_calls");
        assert!(matches!(
            serde_json::from_value::<ControlUnit>(legacy_wire).unwrap(),
            ControlUnit::RunFinished {
                indeterminate_tool_calls: None,
                ..
            }
        ));
    }

    #[test]
    fn caller_wire_reads_legacy_provider_unavailable_spelling() {
        let unit: ControlUnit = serde_json::from_value(serde_json::json!({
            "type": "error",
            "error": {
                "class": "provider_unavailable",
                "message": "provider stream disconnected"
            }
        }))
        .unwrap();

        let ControlUnit::Error { error } = unit else {
            panic!("expected error control unit");
        };
        assert_eq!(
            error.as_inner().class,
            ErrorClass::ProviderUnavailable,
            "legacy raw class remains readable before a caller response is re-serialized"
        );
    }

    #[test]
    fn unknown_caller_error_class_is_preserved_with_a_non_retrying_fallback() {
        let unit: ControlUnit = serde_json::from_value(serde_json::json!({
            "type": "error",
            "error": {"class": "future_class", "message": "new provider outcome"}
        }))
        .unwrap();
        let ControlUnit::Error { error } = unit else {
            panic!("expected error control unit")
        };

        assert!(error.has_unknown_class());
        assert_eq!(error.class_wire(), "future_class");
        assert_eq!(error.as_inner().class, ErrorClass::Permanent);
        assert_eq!(
            serde_json::to_value(&error).unwrap()["class"],
            "future_class"
        );
    }

    #[test]
    fn unknown_control_event_uses_the_display_reset_fallback() {
        let unit: ControlUnit = serde_json::from_value(serde_json::json!({
            "type": "future_control_unit",
            "payload": {"run_id": "run-1"}
        }))
        .unwrap();
        assert_eq!(unit, ControlUnit::DisplayReset);
    }

    #[test]
    fn unknown_finish_and_pause_reasons_decode_as_explicit_unknown_states() {
        let finish: OpenRunFinishReason = serde_json::from_str("\"future_finish\"").unwrap();
        assert_eq!(finish, OpenState::Unknown("future_finish".into()));
        assert_eq!(finish.as_str(), "future_finish");
        assert_eq!(finish.known(), None);
        assert_eq!(serde_json::to_string(&finish).unwrap(), "\"future_finish\"");

        let pause: OpenPauseReason = serde_json::from_str("\"future_pause\"").unwrap();
        assert_eq!(pause, OpenState::Unknown("future_pause".into()));
        assert_eq!(pause.as_str(), "future_pause");
        assert_eq!(serde_json::to_string(&pause).unwrap(), "\"future_pause\"");

        // A malformed value is not a newer vocabulary, so it is still refused.
        assert!(serde_json::from_str::<OpenPauseReason>("7").is_err());
    }

    #[test]
    fn known_open_states_keep_the_closed_wire_spelling() {
        for reason in [
            RunFinishReason::Completed,
            RunFinishReason::MaxSteps,
            RunFinishReason::BudgetExhausted,
            RunFinishReason::ContextCeiling,
            RunFinishReason::Cancelled,
            RunFinishReason::Interrupted,
            RunFinishReason::Error,
            RunFinishReason::TransformUnavailable,
        ] {
            let closed = serde_json::to_string(&reason).unwrap();
            assert_eq!(closed, format!("\"{}\"", reason.as_str()));
            let open = OpenRunFinishReason::from(reason);
            assert_eq!(serde_json::to_string(&open).unwrap(), closed);
            assert_eq!(
                serde_json::from_str::<OpenRunFinishReason>(&closed).unwrap(),
                OpenState::Known(reason)
            );
        }
        for reason in [
            PauseReason::AuthRequired,
            PauseReason::Restart,
            PauseReason::ToolHeld,
        ] {
            let closed = serde_json::to_string(&reason).unwrap();
            assert_eq!(closed, format!("\"{}\"", reason.as_str()));
            let open = OpenPauseReason::from(reason);
            assert_eq!(serde_json::to_string(&open).unwrap(), closed);
            assert_eq!(
                serde_json::from_str::<OpenPauseReason>(&closed).unwrap(),
                OpenState::Known(reason)
            );
        }
    }

    #[test]
    fn durable_reason_enums_stay_closed_to_unknown_values() {
        // WAL records decode these enums directly. A record a newer runner wrote with a reason
        // this build does not know must be refused, not read as some other recorded outcome.
        assert!(serde_json::from_str::<RunFinishReason>("\"future_finish\"").is_err());
        assert!(serde_json::from_str::<PauseReason>("\"future_pause\"").is_err());
        let paused = serde_json::from_value::<ControlUnit>(serde_json::json!({
            "type": "paused",
            "run_id": "run-1",
            "reason": "future_pause"
        }));
        assert!(
            paused.is_err(),
            "a paused unit has no unknown reason to carry; the subscribe reader handles it"
        );
    }

    #[test]
    fn tool_held_pause_reason_is_spelled_tool_held() {
        // The WAL, the control stream and run status all carry this exact spelling.
        assert_eq!(PauseReason::ToolHeld.as_str(), "tool_held");
        assert_eq!(
            serde_json::to_string(&PauseReason::ToolHeld).unwrap(),
            "\"tool_held\""
        );
        assert_eq!(
            serde_json::from_str::<PauseReason>("\"tool_held\"").unwrap(),
            PauseReason::ToolHeld
        );
    }
}
