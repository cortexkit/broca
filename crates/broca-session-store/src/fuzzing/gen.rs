//! Generated values of every stored type, for the property tests. A value is generated from a
//! byte string, as a fuzz input is read, so proptest shrinks a failing case by shrinking its
//! bytes, and every field takes the shapes that break codecs: absent and zero, empty and long
//! strings and bytes, base64 runs of either alphabet, and the largest numbers.

use std::collections::BTreeMap;

use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{
    ContentBlock, EncryptedReasoningFormat, FinishReason, MediaKind, MediaRef, Message, Role,
    TextProviderFormat, TextProviderState, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;

use super::Input;
use crate::codec::{ByteBuf, Digest, Id128};

/// The source a value is generated from.
pub struct G<'a> {
    input: Input<'a>,
    /// How deep the JSON value being generated is nested.
    depth: u32,
}

impl<'a> G<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            input: Input::new(bytes),
            depth: 0,
        }
    }

    pub fn byte(&mut self) -> u8 {
        self.input.byte()
    }

    pub fn flag(&mut self) -> bool {
        self.byte() & 1 == 1
    }

    /// A collection's length: empty or short mostly, sometimes longer.
    pub fn len(&mut self) -> usize {
        match self.byte() % 16 {
            0..=5 => 0,
            6..=12 => 1 + usize::from(self.byte() % 3),
            _ => 4 + usize::from(self.byte() % 5),
        }
    }

    fn u64(&mut self) -> u64 {
        self.input.u64()
    }

    fn bytes(&mut self) -> Vec<u8> {
        match self.byte() % 6 {
            0 => Vec::new(),
            1 => vec![0; 1 + usize::from(self.byte())],
            2 => (0..=255).collect(),
            _ => self.input.chunk().to_vec(),
        }
    }
}

/// A type values can be generated for.
pub trait Gen: Sized {
    fn gen(g: &mut G<'_>) -> Self;
}

/// A generated value of the type the context asks for.
pub fn gen<T: Gen>(g: &mut G<'_>) -> T {
    T::gen(g)
}

macro_rules! unsigned {
    ($($ty:ty),*) => {$(
        impl Gen for $ty {
            fn gen(g: &mut G<'_>) -> Self {
                match g.byte() % 8 {
                    0 => 0,
                    1 => 1,
                    2 => <$ty>::MAX,
                    3 => <$ty>::MAX - 1,
                    4 => g.byte().into(),
                    5 => (1 << (<$ty>::BITS - 1)),
                    _ => g.u64() as $ty,
                }
            }
        }
    )*};
}
unsigned!(u16, u32, u64);

impl Gen for bool {
    fn gen(g: &mut G<'_>) -> Self {
        g.flag()
    }
}

impl Gen for () {
    fn gen(_: &mut G<'_>) -> Self {}
}

const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL_SAFE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// `bytes` as padded base64 in `alphabet`.
fn base64(bytes: &[u8], alphabet: &[u8; 64]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                alphabet[((n >> (18 - 6 * i)) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

impl Gen for String {
    fn gen(g: &mut G<'_>) -> Self {
        match g.byte() % 10 {
            0 | 1 => String::new(),
            2 => "a".repeat(1 + usize::from(g.byte() % 8)),
            3 => "é😀\u{0}\"\\\n\u{7f}\u{2028}".into(),
            4 => "long ".repeat(64 + usize::from(g.byte())),
            // Opaque provider text: a base64 run of either alphabet, long enough to be stored as
            // bytes, with text around it that may itself be base64 characters.
            5 | 6 => {
                let alphabet = if g.flag() { STANDARD } else { URL_SAFE };
                let mut bytes: Vec<u8> = (0..48 + usize::from(g.byte()))
                    .map(|i| (i as u8).wrapping_mul(37) ^ g.byte())
                    .collect();
                if g.flag() {
                    bytes.pop();
                }
                let before =
                    ["", "sig:", "A", "{\"encrypted_content\":\""][usize::from(g.byte() % 4)];
                let after = ["", "\"}", "Z", "=x"][usize::from(g.byte() % 4)];
                format!("{before}{}{after}", base64(&bytes, alphabet))
            }
            // Base64 characters whose run doesn't re-encode exactly: stays text.
            7 => "QUJD".repeat(17) + "R",
            _ => String::from_utf8_lossy(g.input.chunk()).into_owned(),
        }
    }
}

impl Gen for ByteBuf {
    fn gen(g: &mut G<'_>) -> Self {
        ByteBuf(g.bytes())
    }
}

impl Gen for Digest {
    fn gen(g: &mut G<'_>) -> Self {
        let first = g.byte();
        Digest(std::array::from_fn(|i| first.wrapping_add(i as u8)))
    }
}

impl Gen for Id128 {
    fn gen(g: &mut G<'_>) -> Self {
        let first = g.byte();
        Id128(std::array::from_fn(|i| first.wrapping_mul(i as u8 + 1)))
    }
}

impl<T: Gen> Gen for Option<T> {
    fn gen(g: &mut G<'_>) -> Self {
        g.flag().then(|| T::gen(g))
    }
}

impl<T: Gen> Gen for Vec<T> {
    fn gen(g: &mut G<'_>) -> Self {
        (0..g.len()).map(|_| T::gen(g)).collect()
    }
}

impl<T: Gen> Gen for Box<T> {
    fn gen(g: &mut G<'_>) -> Self {
        Box::new(T::gen(g))
    }
}

impl Gen for serde_json::Value {
    fn gen(g: &mut G<'_>) -> Self {
        use serde_json::Value;
        let leaf = g.depth >= 3;
        match g.byte() % if leaf { 6 } else { 8 } {
            0 => Value::Null,
            1 => Value::Bool(g.flag()),
            2 => Value::from(u64::gen(g)),
            3 => Value::from(-(i64::from(u32::gen(g))) - 1),
            4 => {
                let float = f64::from_bits(g.u64());
                let float = if float.is_finite() { float } else { -0.0 };
                serde_json::Number::from_f64(float).map_or(Value::Null, Value::Number)
            }
            5 => Value::String(String::gen(g)),
            6 => {
                g.depth += 1;
                let items = (0..g.len()).map(|_| Value::gen(g)).collect();
                g.depth -= 1;
                Value::Array(items)
            }
            _ => {
                g.depth += 1;
                let map = (0..g.len())
                    .map(|_| (String::gen(g), Value::gen(g)))
                    .collect::<serde_json::Map<_, _>>();
                g.depth -= 1;
                Value::Object(map)
            }
        }
    }
}

/// A generated value of one of a field-less enum's variants.
macro_rules! variants {
    ($ty:ty: $($variant:expr),+ $(,)?) => {
        impl Gen for $ty {
            fn gen(g: &mut G<'_>) -> Self {
                let all = [$($variant),+];
                all[usize::from(g.byte()) % all.len()]
            }
        }
    };
}

/// A generated value of a struct, field by field, every field generated for its own type.
macro_rules! fields {
    ($ty:ident { $($field:ident),* $(,)? }) => {
        impl $crate::fuzzing::gen::Gen for $ty {
            fn gen(g: &mut $crate::fuzzing::gen::G<'_>) -> Self {
                $ty { $($field: $crate::fuzzing::gen::gen(g)),* }
            }
        }
    };
}
pub(crate) use fields;

variants!(Role: Role::System, Role::User, Role::Assistant, Role::Tool);
variants!(MediaKind: MediaKind::Image, MediaKind::Document, MediaKind::Audio, MediaKind::Video);
variants!(
    EncryptedReasoningFormat: EncryptedReasoningFormat::OpenAiResponses,
    EncryptedReasoningFormat::OpenAiResponsesChatgpt,
    EncryptedReasoningFormat::OpenRouter,
);
variants!(
    TextProviderFormat: TextProviderFormat::OpenAiResponses,
    TextProviderFormat::OpenAiResponsesChatgpt,
    TextProviderFormat::Gemini,
    TextProviderFormat::GoogleCodeAssist,
);
variants!(
    FinishReason: FinishReason::Stop,
    FinishReason::Length,
    FinishReason::ToolCalls,
    FinishReason::ContentFilter,
    FinishReason::Error,
    FinishReason::Other,
);
variants!(
    RunFinishReason: RunFinishReason::Completed,
    RunFinishReason::MaxSteps,
    RunFinishReason::BudgetExhausted,
    RunFinishReason::ContextCeiling,
    RunFinishReason::Cancelled,
    RunFinishReason::Interrupted,
    RunFinishReason::Error,
    RunFinishReason::TransformUnavailable,
);
variants!(
    PauseReason: PauseReason::AuthRequired,
    PauseReason::Restart,
    PauseReason::AccountLimit,
    PauseReason::ToolHeld,
);

impl Gen for MessageId {
    fn gen(g: &mut G<'_>) -> Self {
        MessageId(gen(g))
    }
}

impl Gen for ToolCallId {
    fn gen(g: &mut G<'_>) -> Self {
        ToolCallId(gen(g))
    }
}

impl Gen for ModelHandle {
    fn gen(g: &mut G<'_>) -> Self {
        ModelHandle {
            provider_module_id: gen(g),
            model_id: gen(g),
        }
    }
}

impl Gen for Usage {
    fn gen(g: &mut G<'_>) -> Self {
        Usage {
            input_tokens: gen(g),
            cached_input_tokens: gen(g),
            cache_write_tokens: gen(g),
            cache_write_5m_tokens: gen(g),
            cache_write_1h_tokens: gen(g),
            output_tokens: gen(g),
            reasoning_tokens: gen(g),
        }
    }
}

impl Gen for MediaRef {
    fn gen(g: &mut G<'_>) -> Self {
        let kind: MediaKind = gen(g);
        // Only images have dimensions, and an image has both.
        let (width, height) = if kind == MediaKind::Image {
            (Some(gen(g)), Some(gen(g)))
        } else {
            (None, None)
        };
        MediaRef {
            kind,
            mime: gen(g),
            byte_size: gen(g),
            sha256: gen(g),
            width,
            height,
        }
    }
}

impl Gen for TextProviderState {
    fn gen(g: &mut G<'_>) -> Self {
        TextProviderState {
            format: gen(g),
            data: gen(g),
        }
    }
}

impl Gen for ToolCall {
    fn gen(g: &mut G<'_>) -> Self {
        ToolCall {
            tool_call_id: gen(g),
            tool_name: gen(g),
            input: gen(g),
            thought_signature: gen(g),
        }
    }
}

impl Gen for ToolOutputPart {
    fn gen(g: &mut G<'_>) -> Self {
        if g.flag() {
            ToolOutputPart::Text { text: gen(g) }
        } else {
            ToolOutputPart::Media { media: gen(g) }
        }
    }
}

impl Gen for ToolOutput {
    fn gen(g: &mut G<'_>) -> Self {
        match g.byte() % 5 {
            0 => ToolOutput::Text { text: gen(g) },
            1 => ToolOutput::Json { value: gen(g) },
            2 => ToolOutput::Parts { parts: gen(g) },
            3 => ToolOutput::Denied { reason: gen(g) },
            _ => ToolOutput::Error {
                message: gen(g),
                module: gen(g),
                code: gen(g),
                detail: gen(g),
                nothing_sent: gen(g),
                retry: gen(g),
            },
        }
    }
}

impl Gen for ToolResult {
    fn gen(g: &mut G<'_>) -> Self {
        ToolResult {
            tool_call_id: gen(g),
            output: gen(g),
            is_error: gen(g),
        }
    }
}

impl Gen for ContentBlock {
    fn gen(g: &mut G<'_>) -> Self {
        match g.byte() % 8 {
            0 => ContentBlock::Media(gen(g)),
            1 => ContentBlock::Text { text: gen(g) },
            2 => ContentBlock::TextWithState {
                text: gen(g),
                provider_state: gen(g),
            },
            3 => ContentBlock::Reasoning {
                text: gen(g),
                signature: gen(g),
            },
            4 => ContentBlock::RedactedReasoning {
                data: gen(g),
                provider_format: gen(g),
            },
            5 => ContentBlock::ToolCall(gen(g)),
            6 => ContentBlock::ToolResult(gen(g)),
            _ => ContentBlock::ProviderFallback {
                from_model: gen(g),
                to_model: gen(g),
                raw: gen(g),
            },
        }
    }
}

impl Gen for Message {
    fn gen(g: &mut G<'_>) -> Self {
        // `ordinal` and `tags` are derived when a prompt is assembled and never stored, so a
        // stored message always has them unset.
        let mut message = Message::new(gen(g), gen(g));
        message.turn_system = gen(g);
        message.cache_prefix_blocks = gen(g);
        message.origin = gen(g);
        message
    }
}

impl<K: Gen + Ord, V: Gen> Gen for BTreeMap<K, V> {
    fn gen(g: &mut G<'_>) -> Self {
        (0..g.len()).map(|_| (gen(g), gen(g))).collect()
    }
}

// The records of a session log.

mod records {
    use super::{gen, Gen, G};
    use crate::prune::{OpaqueField, PrunedOpaque};
    use crate::record::*;

    variants!(
        Placement: Placement::RunInput,
        Placement::Steer,
        Placement::Delivered,
        Placement::BudgetNudge,
        Placement::Imported,
        Placement::DeliveredTurnSystem,
        Placement::DamageNotice,
    );
    variants!(
        PartKind: PartKind::Tools,
        PartKind::ProviderAnswer,
        PartKind::SystemText,
        PartKind::Plan,
        PartKind::Declarations,
        PartKind::ResponseFormat,
        PartKind::GlobalSystem,
        PartKind::RenderConfig,
    );
    variants!(
        CallOutcome: CallOutcome::Completed,
        CallOutcome::Unknown,
        CallOutcome::Interrupted,
        CallOutcome::MaxSteps,
        CallOutcome::NotDispatched,
    );
    variants!(BudgetFiring: BudgetFiring::Nudge, BudgetFiring::Finalize);
    variants!(
        Unanswered: Unanswered::Denied,
        Unanswered::NotExecuted,
        Unanswered::Interrupted,
        Unanswered::Cancelled,
    );
    variants!(CancelPoint: CancelPoint::Now, CancelPoint::StepBoundary);
    variants!(
        ChangeKind: ChangeKind::Cut,
        ChangeKind::LedgerSeal,
        ChangeKind::Revert,
        ChangeKind::Cold,
        ChangeKind::Recover,
    );
    variants!(
        OpaqueField: OpaqueField::ReasoningSignature,
        OpaqueField::RedactedReasoningData,
        OpaqueField::TextProviderState,
        OpaqueField::ToolCallThoughtSignature,
    );

    crate::fuzzing::gen::fields!(PrunedOpaque {
        block,
        field,
        size,
        digest
    });
    crate::fuzzing::gen::fields!(Notice { sender, template });
    crate::fuzzing::gen::fields!(Input {
        run_id,
        placement,
        message,
        written_at_ms,
        submission_id,
        notice,
        tags_v1,
        receipt_seq,
        pruned,
        media,
    });
    crate::fuzzing::gen::fields!(Assistant {
        run_id,
        step_id,
        provider_message_id,
        message,
        usage,
        written_at_ms,
        last_content_at_ms,
        tags_v1,
        budget_finalize,
        pruned,
    });
    crate::fuzzing::gen::fields!(ToolReply {
        run_id,
        batch_id,
        result,
        finished_at_ms,
        tags_v1,
        media,
    });
    crate::fuzzing::gen::fields!(MediaObject {
        part,
        log,
        generation,
        seq
    });
    crate::fuzzing::gen::fields!(InputMedia {
        block,
        part,
        log,
        generation,
        seq
    });
    crate::fuzzing::gen::fields!(SendMedia {
        message,
        block,
        part,
        log,
        generation,
        seq
    });
    crate::fuzzing::gen::fields!(ImportedIds {
        log,
        generation,
        seq
    });
    crate::fuzzing::gen::fields!(ImportedId { mid, ordinal });
    crate::fuzzing::gen::fields!(ImportedIdMap { ids });
    crate::fuzzing::gen::fields!(ImportedMove { mid, ordinal });
    crate::fuzzing::gen::fields!(ImportReceipt {
        import_id,
        digest,
        shape,
        moves,
        imported_messages,
        dropped_system_messages,
    });
    crate::fuzzing::gen::fields!(TurnFacts { ordinal, calls });
    crate::fuzzing::gen::fields!(CallFacts {
        tool_call_id,
        call_key,
        schema_pin,
        carrier,
        dispatched_to,
        dispatched_at_ms,
        finished_at_ms,
        unanswered,
    });
    crate::fuzzing::gen::fields!(ResultAppend {
        run_id,
        tool_call_id,
        text,
        notice,
        written_at_ms,
        send_id,
        ride,
    });
    variants!(RideForm: RideForm::Append);
    crate::fuzzing::gen::fields!(RideEnvelope {
        call_key,
        sender,
        forms,
        form,
        speaks_for,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(StepUsage {
        prompt_tokens,
        cache_read_tokens,
        cache_write_tokens,
        output_tokens,
        reasoning_tokens,
    });
    crate::fuzzing::gen::fields!(ViewRef { version, checksum });
    crate::fuzzing::gen::fields!(EntryFacts {
        send_id,
        ordinal,
        written_at_ms,
        tag_base,
        notice,
        usage,
        generated_under,
        native,
        render_tags,
        clean_reply,
        mid,
        run_id,
        created_at_ms,
        delivery_mode,
        delivery_tool_call_id,
        client_message_id,
        sender,
        marks,
        no_display_facts,
        reasoning_summaries,
        rich_ui,
        notices,
        runner_tag_base,
        legacy_view_seq,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(RideNotice {
        part,
        send_id,
        sender,
        call_key,
        text,
        speaks_for,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(ReasoningSummary { part, text });
    crate::fuzzing::gen::fields!(RichUi {
        dialect,
        catalog_digest
    });
    crate::fuzzing::gen::fields!(PartRef { kind, digest });
    crate::fuzzing::gen::fields!(SetupStub {
        parts,
        model,
        tool_names,
        render_key,
    });
    crate::fuzzing::gen::fields!(RunStart {
        run_id,
        model,
        setup,
        renderer_version,
        submission_id,
        send_id,
        request_identity_digest,
        ts_ms,
        written_at_ms,
        session_class,
        retention_secs,
        title,
        context_limit,
        service_tier,
        route_decision_id,
        compaction_plan_version,
        auth_selection,
        run_choices,
        origin,
        principal,
        scope,
        hooks,
        keep_warm,
        on_restart,
        seeded,
        refusal_schedule,
        imported_ids,
        message_marks,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(RunEnd {
        run_id,
        reason,
        usage,
        ts_ms,
        retries_used,
        interrupt_reason,
        error,
    });
    crate::fuzzing::gen::fields!(StepStart {
        run_id,
        step_id,
        mid
    });
    crate::fuzzing::gen::fields!(StepEnd {
        run_id,
        step_id,
        usage,
        finish_reason,
        retries_used,
        service_tier_used,
        speed_used,
        call_count,
        fallback,
        hooks,
        budget_finalize,
        total_usage,
        stop_met,
        limiting_stop_met,
        by_model,
    });
    crate::fuzzing::gen::fields!(ModelStepUsage {
        provider,
        model,
        variant,
        usage,
    });
    crate::fuzzing::gen::fields!(Attempt {
        run_id,
        step_id,
        attempt,
        attempt_id,
        outcome,
        http_status,
        request_body_len,
        request_body_sha256,
        response_body_len,
        response_body_sha256,
        ts_ms,
        sent_at_ms,
        first_content_at_ms,
        last_content_at_ms,
        transport,
        served_account_ref,
        billing,
        telemetry,
        reached_cache,
        requested_cache_ttl_ms
    });
    crate::fuzzing::gen::fields!(BatchStart {
        run_id,
        batch_id,
        step_id,
        planned,
        mid
    });
    crate::fuzzing::gen::fields!(BatchEnd { run_id, batch_id });
    crate::fuzzing::gen::fields!(ToolIntent {
        run_id,
        batch_id,
        tool_call_id,
        tool_name,
        module_id,
        args_digest,
        args,
        call_key,
        approval_digest,
        dispatched_at_ms,
        schema_pin,
        hooks,
        key_derived,
    });
    crate::fuzzing::gen::fields!(ToolOutcome {
        run_id,
        batch_id,
        tool_call_id,
        outcome,
        result_digest,
        is_error,
        reason,
        finished_at_ms,
        detail,
        hooks,
        call_key,
    });
    crate::fuzzing::gen::fields!(Compaction { version, body });
    crate::fuzzing::gen::fields!(Marker {
        view_version,
        first_kept_mid,
        first_kept_ordinal,
        checkpoint,
        harness,
        session,
    });
    crate::fuzzing::gen::fields!(View {
        version,
        checksum,
        edits,
        built_through,
        new_signed,
        applied_by
    });
    crate::fuzzing::gen::fields!(ViewApplied {
        step_id,
        version,
        checksum
    });
    crate::fuzzing::gen::fields!(QueuedSend {
        submission_id,
        send_id,
        input,
        setup,
        written_at_ms,
        ts_ms,
        request,
        receipt,
        delivery,
        media,
        message_marks,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(SessionRefresh {
        generation,
        policy,
        run_in_progress,
        applies_from_next_run,
        setup,
        manifest,
        selection,
    });
    crate::fuzzing::gen::fields!(StructuredResult { run_id, result });
    crate::fuzzing::gen::fields!(Resumed {
        run_id,
        resumed_from_seq,
        messages,
        model_steps,
        tool_results,
        redone,
        indeterminate,
    });
    crate::fuzzing::gen::fields!(Redo {
        step_id,
        tool_call_id
    });
    crate::fuzzing::gen::fields!(Pause {
        run_id,
        reason,
        ts_ms,
        error,
        account_limit,
    });
    crate::fuzzing::gen::fields!(ToolHeld {
        batch_id,
        tool_call_id,
        call_key,
        module,
        reason,
        detail_digest,
        held_at_ms,
        sequential,
    });
    crate::fuzzing::gen::fields!(BudgetFire {
        run_id,
        firing,
        threshold,
        ordinal,
        caller_text,
        tags_v1,
        nudge_index,
    });
    crate::fuzzing::gen::fields!(Steer {
        input,
        written_at_ms,
        tags_v1,
        receipt,
        media,
        message_marks,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(Delivery {
        submission_id,
        run_id,
        mode,
        input,
        tool_call_id,
        appended_text,
        written_at_ms,
        tags_v1,
        input_listed,
    });
    crate::fuzzing::gen::fields!(Retraction {
        submission_id,
        failure
    });
    crate::fuzzing::gen::fields!(SendEnded {
        submission_id,
        reason
    });
    crate::fuzzing::gen::fields!(DeliveryOrdered {
        submission_id,
        order,
        sent_by
    });
    crate::fuzzing::gen::fields!(CeilingDecided { step_id, detail });
    crate::fuzzing::gen::fields!(OutputCapLowered {
        step_id,
        max_output_tokens,
        estimated_input_tokens,
        context_limit,
    });
    crate::fuzzing::gen::fields!(CancelRequest { run_id, at });
    crate::fuzzing::gen::fields!(Event { name, run_id, body });
    crate::fuzzing::gen::fields!(WriterOpened { generation });
    crate::fuzzing::gen::fields!(ChangeIntent { change, kind, cut });
    crate::fuzzing::gen::fields!(TranscriptReverted {
        change,
        from_seq,
        through_seq,
        ordinal,
        history_version,
        tag_high,
    });
    crate::fuzzing::gen::fields!(RecoveryCut {
        change,
        at,
        durable_bound,
        removed_from,
        removed_through,
        loss_aware,
        transcript_from,
        ordinal,
        history_version,
        unattributed_risk,
        saved,
    });
    crate::fuzzing::gen::fields!(ChangeAborted { change });
    crate::fuzzing::gen::fields!(StructuredDowngraded { run_id });
    crate::fuzzing::gen::fields!(Warmed {
        run_id,
        step_id,
        trigger,
        request_body_sha256,
        warm_body_sha256,
        usage,
        served_account_ref,
        sent_at_ms,
        ttl_ms,
        expires_at_ms,
        attempt_id,
        billing,
        ratelimit_headers,
        reached_cache,
        requested_cache_ttl_ms,
    });
    crate::fuzzing::gen::fields!(ConfigRefrozen {
        run_id,
        ts_ms,
        renderer_version,
        render_config,
    });
    crate::fuzzing::gen::fields!(AuthRefrozen {
        run_id,
        ts_ms,
        auth_selection
    });
    crate::fuzzing::gen::fields!(StepRefused {
        run_id,
        step_id,
        usage,
        served_model,
        usage_iterations,
        discarded_blocks,
        schedule,
        ts_ms,
        by_model,
    });
    crate::fuzzing::gen::fields!(Billed { run_id });
    crate::fuzzing::gen::fields!(RefreshPolicy {
        generation,
        policy,
        send_id,
    });
    crate::fuzzing::gen::fields!(PrefixFlush {
        send_id,
        pending_generation,
    });
    crate::fuzzing::gen::fields!(RefreshApplied {
        generation,
        applied_by,
        manifest_digest,
        composition_digest,
        absent_items,
    });
    crate::fuzzing::gen::fields!(CompactionEnrolled {
        through_ordinal,
        tag_floor,
        tag_high,
        overlay,
    });
    crate::fuzzing::gen::fields!(Moved {
        move_id,
        source_activity_ms,
        source_lineage,
    });
    crate::fuzzing::gen::fields!(EnrollmentOverlayRef {
        log,
        generation,
        seq
    });
    crate::fuzzing::gen::fields!(EnrolledEntry {
        ordinal,
        tag_base,
        written_at_ms,
        suppressed,
        notice,
    });
    crate::fuzzing::gen::fields!(EnrolledEntries { entries });

    /// Every record kind, in kind order: a record of kind `KINDS[n]` is generated by
    /// `record_of(n, ...)`.
    pub const KINDS: [u32; 52] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
        80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
        102, 103, 104, 105, 106,
    ];

    /// A generated record of the `n`th kind in [`KINDS`].
    pub fn record_of(n: usize, g: &mut G<'_>) -> Record {
        match n % KINDS.len() {
            0 => Record::Input(gen(g)),
            1 => Record::Assistant(gen(g)),
            2 => Record::ToolReply(gen(g)),
            3 => Record::ResultAppend(gen(g)),
            4 => Record::SetupStub(gen(g)),
            5 => Record::Compaction(gen(g)),
            6 => Record::Marker(gen(g)),
            7 => Record::EntryFacts(gen(g)),
            8 => Record::TurnFacts(gen(g)),
            9 => Record::RunStart(gen(g)),
            10 => Record::RunEnd(gen(g)),
            11 => Record::StepStart(gen(g)),
            12 => Record::StepEnd(gen(g)),
            13 => Record::Attempt(gen(g)),
            14 => Record::BatchStart(gen(g)),
            15 => Record::BatchEnd(gen(g)),
            16 => Record::ToolIntent(gen(g)),
            17 => Record::ToolOutcome(gen(g)),
            18 => Record::QueuedSend(gen(g)),
            19 => Record::SessionRefresh(gen(g)),
            20 => Record::StructuredResult(gen(g)),
            21 => Record::Event(gen(g)),
            22 => Record::Pause(gen(g)),
            23 => Record::ToolHeld(gen(g)),
            24 => Record::BudgetFire(gen(g)),
            25 => Record::Steer(gen(g)),
            26 => Record::Delivery(gen(g)),
            27 => Record::CancelRequest(gen(g)),
            28 => Record::Retraction(gen(g)),
            29 => Record::SendEnded(gen(g)),
            30 => Record::DeliveryOrdered(gen(g)),
            31 => Record::View(gen(g)),
            32 => Record::ViewApplied(gen(g)),
            33 => Record::WriterOpened(gen(g)),
            34 => Record::ChangeIntent(gen(g)),
            35 => Record::ChangeAborted(gen(g)),
            36 => Record::TranscriptReverted(gen(g)),
            37 => Record::RecoveryCut(gen(g)),
            38 => Record::Resumed(gen(g)),
            39 => Record::CeilingDecided(gen(g)),
            40 => Record::OutputCapLowered(gen(g)),
            41 => Record::StructuredDowngraded(gen(g)),
            42 => Record::Warmed(gen(g)),
            43 => Record::ConfigRefrozen(gen(g)),
            44 => Record::AuthRefrozen(gen(g)),
            45 => Record::StepRefused(gen(g)),
            46 => Record::Billed(gen(g)),
            47 => Record::RefreshPolicy(gen(g)),
            48 => Record::PrefixFlush(gen(g)),
            49 => Record::RefreshApplied(gen(g)),
            50 => Record::CompactionEnrolled(gen(g)),
            _ => Record::Moved(gen(g)),
        }
    }

    impl Gen for Record {
        fn gen(g: &mut G<'_>) -> Self {
            let n = usize::from(g.byte());
            record_of(n, g)
        }
    }
}

pub use records::{record_of, KINDS};

// The operational fold's state. The conversation fold's state has a private member, so its
// generator lives in `fold::conversation`.

mod operations {
    use crate::fold::operations::*;

    crate::fuzzing::gen::fields!(Call {
        id,
        intent,
        held,
        terminal,
        key,
        held_reply,
        args
    });
    crate::fuzzing::gen::fields!(OpenBatch {
        batch_id,
        step_id,
        calls,
        results_mid
    });
    crate::fuzzing::gen::fields!(PendingInput {
        send_id,
        seq,
        placement,
        messages,
        written_at_ms,
        tags_v1,
        submission_id,
        sender,
        request_identity_digest,
        media,
        message_marks,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(NaturalEnd { stop, extendable });
    crate::fuzzing::gen::fields!(Budget {
        nudges_fired,
        finalize_fired,
        finalize_pending,
    });
    crate::fuzzing::gen::fields!(Run {
        run_id,
        finished,
        paused,
        seeded,
        completed_steps,
        tool_call_count,
        usage,
        retries_used,
        last_finish_reason,
        natural_end,
        open_batch,
        unknown,
        hard_stopped,
        pending_steers,
        cancel,
        budget,
        last_step_usage,
        provider_call_started,
        account_limit_pause,
        keep_warm,
        on_restart,
        start,
        context_ceiling,
        output_cap,
        structured_downgraded,
        warms,
        refrozen_render,
        refrozen_auth,
        refused_step,
        step_request,
        step_attempt,
        end_error,
        end_usage,
        step_mid
    });
    crate::fuzzing::gen::fields!(StepAttempt { step_id, ts_ms });
    crate::fuzzing::gen::fields!(StepRequest {
        step_id,
        sent_at_ms
    });
    crate::fuzzing::gen::fields!(OutputCap {
        step_id,
        max_output_tokens
    });
    crate::fuzzing::gen::fields!(OutputMeasure {
        input_tokens,
        request_bytes,
        model,
    });
    crate::fuzzing::gen::fields!(ViewSlot { version, checksum });
    crate::fuzzing::gen::fields!(AppliedView { step_id, view });
    crate::fuzzing::gen::fields!(Pending {
        send_id,
        submission_id,
        interrupt,
        ordered,
        sender,
        request_identity_digest,
        message_marks,
        named_speaker,
    });
    crate::fuzzing::gen::fields!(Operations {
        run,
        refusal_schedule,
        queue,
        view_accepted,
        view_applied,
        open_change,
        transcript_changed,
        history_version,
        unattributed_risk,
        setup,
        ordered_next,
        estimate,
        last_send_run,
        last_cache_send,
        first_protocol_compaction,
        output_measure,
        output_attempt_bytes,
        last_cache_attempt_digest,
        owner,
        session_usage,
        billing_owed,
        imported_ids,
        title,
        plan,
        enrolled,
        tag_high_water,
        source_lineage
    });
    crate::fuzzing::gen::fields!(Enrollment {
        through_ordinal,
        tag_floor,
        tag_high,
        overlay,
        run_id
    });
    crate::fuzzing::gen::fields!(Plan {
        accepted_generation,
        applied_generation,
        pending,
        effective,
        current,
        prefix,
        rebuild_since_accept,
        flushed_since_accept,
        last_model,
        accepted,
        selection,
    });
    crate::fuzzing::gen::fields!(PendingChange {
        generation,
        policy,
        manifest
    });
    crate::fuzzing::gen::fields!(EffectiveChange {
        generation,
        applied_by,
        manifest_digest,
        composition_digest,
        absent_items,
    });
    crate::fuzzing::gen::fields!(ManifestRef { parts, rest });
    crate::fuzzing::gen::fields!(SessionUsage {
        since,
        totals,
        by_model
    });
    crate::fuzzing::gen::fields!(UsageTotals {
        steps,
        unreported_steps,
        input_tokens,
        cached_input_tokens,
        cache_write_tokens,
        output_tokens,
        reasoning_tokens
    });
    crate::fuzzing::gen::fields!(ModelTotals {
        provider,
        model,
        variant,
        totals
    });
    crate::fuzzing::gen::fields!(Owner {
        episodes,
        first_principal,
        any_unrecorded_principal,
        principals,
        scope,
        first_start,
        adopted_session_class
    });
    crate::fuzzing::gen::fields!(Estimate {
        run_model,
        step_model,
        step_usage,
        step_fallback,
        stepped,
        current_request_bytes,
        previous_request_bytes,
    });
    crate::fuzzing::gen::fields!(WarmSpend {
        step_id,
        reached_cache,
        usage,
        billing,
        ttl_ms
    });
    crate::fuzzing::gen::fields!(CacheSend {
        sent_at_ms,
        run_id,
        requested_ttl_ms
    });
    crate::fuzzing::gen::fields!(SetupInForce { parts, model });
}

mod stored {
    use super::{Gen, G};
    use crate::checkpoint::Checkpoint;
    use crate::directory::{DirEntry, Page, PageRef};
    use crate::head::*;
    use crate::idindex::*;
    use crate::segment::{BlockBody, BlockEntry, SegmentHeader, SegmentIndex, StoredRecord};

    crate::fuzzing::gen::fields!(Checkpoint {
        covers_through,
        next_seq,
        conversation,
        operational,
        transcript_base,
        ledger_base,
        activity_ms,
    });
    crate::fuzzing::gen::fields!(SessionTriple {
        project_root,
        harness,
        session
    });
    crate::fuzzing::gen::fields!(WrappedKey {
        id,
        deployment,
        wrapped
    });
    crate::fuzzing::gen::fields!(OpenLog { change, synced_len });
    crate::fuzzing::gen::fields!(LastChange { change, intent_seq });
    crate::fuzzing::gen::fields!(PendingDeletion {
        change,
        unreferenced_at_ms,
        logs,
        objects,
        retired_index,
        abandoned,
    });
    impl Gen for Residence {
        fn gen(g: &mut G<'_>) -> Self {
            match g.byte() % 3 {
                0 => Residence::Local(()),
                1 => Residence::Creating(super::gen(g)),
                _ => Residence::Recovering(super::gen(g)),
            }
        }
    }
    crate::fuzzing::gen::fields!(Head {
        format,
        session_id,
        triple,
        incarnation,
        history_version,
        residence,
        features,
        keys,
        generation,
        next_seq,
        open_log,
        last_change,
        transcript_dir,
        ledger_dir,
        pending,
        id_index,
        cold_since_ms,
        appends_in_conversation,
        cipher,
    });
    variants!(
        crate::keys::Cipher: crate::keys::Cipher::XChaCha20Poly1305,
        crate::keys::Cipher::None,
    );
    crate::fuzzing::gen::fields!(DirEntry {
        object,
        first_seq,
        last_seq,
        size,
        sealed_at_ms,
        first_ordinal,
        last_ordinal,
    });
    crate::fuzzing::gen::fields!(PageRef {
        object,
        level,
        first_seq,
        last_seq,
        count,
        first_ordinal,
        last_ordinal,
    });
    crate::fuzzing::gen::fields!(Page {
        level,
        entries,
        children
    });
    variants!(
        IdKind: IdKind::RunId,
        IdKind::SubmissionId,
        IdKind::SendId,
        IdKind::CallKey,
        IdKind::RefreshGeneration,
    );
    crate::fuzzing::gen::fields!(Entry { hash, seq });
    crate::fuzzing::gen::fields!(DataPage { entries });
    crate::fuzzing::gen::fields!(PageInfo {
        object,
        first,
        last,
        count,
        filter
    });
    crate::fuzzing::gen::fields!(RefPage { pages });
    crate::fuzzing::gen::fields!(RefInfo {
        object,
        first,
        last
    });
    crate::fuzzing::gen::fields!(Run { entries, refs });
    crate::fuzzing::gen::fields!(Merge {
        inputs,
        done,
        written,
        next
    });
    crate::fuzzing::gen::fields!(Level { runs, merge });
    crate::fuzzing::gen::fields!(IndexRoot { levels });
    crate::fuzzing::gen::fields!(Retired { refs });
    crate::fuzzing::gen::fields!(StoredRecord { seq, kind, body });
    crate::fuzzing::gen::fields!(BlockBody { records });
    crate::fuzzing::gen::fields!(BlockEntry {
        first_seq,
        offset,
        len,
        records,
        first_ordinal,
    });
    crate::fuzzing::gen::fields!(SegmentHeader {
        format,
        session_id,
        stream,
        first_seq,
        last_seq,
        created_at_ms,
        prefixes,
    });
    crate::fuzzing::gen::fields!(SegmentIndex {
        blocks,
        leading,
        trailer,
        log_start
    });
}
