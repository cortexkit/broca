use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolOutput, ToolResult},
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    BindIdentity, RunFinishReason, RunId, ToolCallId, Usage,
};
use broca_wal::{RunConfig, RunOrigin, SeedOriginKind, WalRecord};
use sha2::{Digest, Sha256};

use crate::{run::epoch_millis, SessionStore, WalHandle, WalStore};

/// Imports stay comfortably below the 64 MiB transport/frame limit, leaving room for
/// the operation envelope and WAL record metadata.
pub const IMPORT_MAX_SERIALIZED_BYTES: usize = 48 * 1024 * 1024;
/// A second sanity bound prevents tiny-message inputs from consuming unbounded memory.
pub const IMPORT_MAX_MESSAGES: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportOrigin {
    pub source_ref: String,
}

#[derive(Debug, Clone)]
pub struct ImportRequest {
    /// The producer's bind project id, recorded on RunStarted for later inspection.
    /// Changing it must not change the identity used to recognize an import retry.
    pub project_id: Option<String>,
    /// The principal the daemon stamped on the importing bind, recorded on RunStarted. Like
    /// `project_id`, it must not change the identity used to recognize an import retry.
    pub principal: Option<broca_protocol::BindPrincipal>,
    pub run_id: RunId,
    pub session: BindIdentity,
    pub messages: Vec<Message>,
    pub origin: ImportOrigin,
}

/// What the import did, including the parts a caller would not otherwise see.
///
/// Normalization rewrites the transcript, and every class of rewrite is counted here. A
/// caller reads this to decide whether the import matched what they handed over, so a
/// rewrite performed but not reported reads to them as no rewrite at all -- which is
/// indistinguishable from a clean import of a transcript that never needed one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportOutcome {
    pub run_id: RunId,
    /// Dangling tool calls given a terminal result so the history is provider-valid.
    pub synthesized_tool_results: usize,
    /// Tool results with no matching call, which nothing could pair.
    pub dropped_orphan_tool_results: usize,
    /// System messages removed. A lineage's system prompt belongs to the session that
    /// continues it, and an imported one would fail the integrity check on the next send.
    /// Reported because a caller who authored a system prompt should learn it did not
    /// survive, rather than discovering it when the next turn behaves differently.
    pub dropped_system_messages: usize,
    pub already_imported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportError {
    Wal(String),
    LineageNotEmpty,
    ImportIdReuse,
    TooManyMessages {
        count: usize,
        max: usize,
    },
    TooLarge {
        bytes: usize,
        max: usize,
    },
    InvalidSourceRef,
    EmptyMessage {
        ordinal: usize,
    },
    /// The request carried no importable messages, either because it arrived empty or because
    /// normalization dropped everything it had (a system-only transcript, or nothing but orphan
    /// tool results). Refused before any WAL write: an empty episode is durable and consumes
    /// the lineage, so the later real import fails `LineageNotEmpty` and the caller is left
    /// with a session that can never be filled.
    NoImportableContent,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wal(error) => write!(f, "wal failure: {error}"),
            // Both of these name what the caller must do next, not just what was refused: a
            // client renders this text verbatim, so a message that only states the rule leaves
            // a reader knowing the import failed and not knowing whether to retry, rename, or
            // pick a different session.
            Self::LineageNotEmpty => write!(
                f,
                "session import requires an empty lineage; this session already has runs, so import into a new session id"
            ),
            Self::ImportIdReuse => write!(
                f,
                "import_id was reused with different messages or origin; a retry must carry byte-identical content and a new import needs a new import_id"
            ),
            Self::TooManyMessages { count, max } => {
                write!(f, "session has {count} messages; import limit is {max}")
            }
            Self::TooLarge { bytes, max } => {
                write!(
                    f,
                    "serialized session is {bytes} bytes; import limit is {max}"
                )
            }
            Self::InvalidSourceRef => {
                write!(
                    f,
                    "import source_ref must be namespaced as '<namespace>:<id>'"
                )
            }
            Self::EmptyMessage { ordinal } => {
                write!(f, "import message at ordinal {ordinal} has no content")
            }
            Self::NoImportableContent => {
                write!(
                    f,
                    "import carried no messages to import, so nothing was written; if the \
                     source transcript holds only system messages or unpaired tool results \
                     there is nothing to seed and the session stays empty and importable"
                )
            }
        }
    }
}

impl std::error::Error for ImportError {}

/// Resolve durable import idempotency without writing records. The active actor supplies its
/// writer so this read-only preflight can reject poisoned or unproven matching terminals.
pub fn check_import_retry<W: WalStore>(
    request: &ImportRequest,
    wal_store: &W,
    writer: &WalHandle,
) -> Result<Option<ImportOutcome>, ImportError> {
    let identity_bytes = serde_json::to_vec(&serde_json::json!({
        "messages": &request.messages,
        "origin": {"kind": "import", "source_ref": &request.origin.source_ref},
    }))
    .map_err(|error| ImportError::Wal(error.to_string()))?;
    let request_digest = format!("{:x}", Sha256::digest(identity_bytes));
    let (messages, counts) = normalize_messages(request.messages.clone());
    let origin = RunOrigin::SeededFrom {
        origin_kind: SeedOriginKind::Import,
        source_ref: request.origin.source_ref.clone(),
        origin_device_pseudonym: None,
        base_seq: None,
    };
    let frames = wal_store
        .read_records(&request.session)
        .map_err(|error| ImportError::Wal(error.to_string()))?;
    let Some((durable_input, durable_origin, durable_config, import_terminal_seq)) =
        complete_import_for(&frames, &request.run_id)
    else {
        return Ok(None);
    };
    let durable_digest = durable_config
        .provider_options
        .get("import_request_digest")
        .and_then(serde_json::Value::as_str);
    if durable_input == messages.as_slice()
        && durable_origin == &origin
        && durable_digest == Some(request_digest.as_str())
    {
        // This actor may have appended complete frames that are readable from page cache while a
        // failed barrier has poisoned its writer. The retry must prove the matching terminal is
        // covered by this writer's durable head rather than acknowledging a readable tail.
        if writer.is_poisoned() {
            return Err(ImportError::Wal(
                writer
                    .poison_error()
                    .unwrap_or_else(|| "session writer is poisoned".into()),
            ));
        }
        if writer
            .durable_head()
            .load(std::sync::atomic::Ordering::Acquire)
            < import_terminal_seq
        {
            return Err(ImportError::Wal(format!(
                "import terminal seq {import_terminal_seq} is beyond the writer's proven durable head"
            )));
        }
        return Ok(Some(ImportOutcome {
            run_id: request.run_id.clone(),
            synthesized_tool_results: counts.synthesized_tool_results,
            dropped_orphan_tool_results: counts.dropped_orphan_tool_results,
            dropped_system_messages: counts.dropped_system_messages,
            already_imported: true,
        }));
    }
    Err(ImportError::ImportIdReuse)
}

/// Write one provider-free synthetic episode without a materialized store.
///
/// The caller owns the session writer and lease; acknowledgement is safe only after this
/// function returns from its single fsync barrier.
pub async fn run_import<W: WalStore>(
    request: ImportRequest,
    writer: &WalHandle,
    wal_store: &W,
) -> Result<ImportOutcome, ImportError> {
    run_import_with_store(request, writer, wal_store, None).await
}

/// Write one provider-free synthetic episode and project it into `store` when configured.
///
/// The caller owns the session writer and lease for the entire call.
pub async fn run_import_with_store<W: WalStore>(
    request: ImportRequest,
    writer: &WalHandle,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
) -> Result<ImportOutcome, ImportError> {
    if !request
        .origin
        .source_ref
        .split_once(':')
        .is_some_and(|(namespace, id)| !namespace.is_empty() && !id.is_empty())
    {
        return Err(ImportError::InvalidSourceRef);
    }
    // Refuse before the digest, the lineage read, or any write. An accepted empty import is
    // durable -- RunStarted{input: []} plus a Completed terminal -- and that consumes the
    // lineage, so every later attempt fails: a new import id on LineageNotEmpty, the same id
    // with real content on ImportIdReuse. The session becomes permanently unfillable, and the
    // request that did it looked successful.
    if request.messages.is_empty() {
        return Err(ImportError::NoImportableContent);
    }
    if request.messages.len() > IMPORT_MAX_MESSAGES {
        return Err(ImportError::TooManyMessages {
            count: request.messages.len(),
            max: IMPORT_MAX_MESSAGES,
        });
    }
    if let Some(ordinal) = request
        .messages
        .iter()
        .position(|message| message.content.is_empty())
    {
        return Err(ImportError::EmptyMessage { ordinal });
    }
    let serialized = serde_json::to_vec(&request.messages)
        .map_err(|error| ImportError::Wal(error.to_string()))?;
    let serialized_bytes = serialized.len();
    if serialized_bytes > IMPORT_MAX_SERIALIZED_BYTES {
        return Err(ImportError::TooLarge {
            bytes: serialized_bytes,
            max: IMPORT_MAX_SERIALIZED_BYTES,
        });
    }

    let identity_bytes = serde_json::to_vec(&serde_json::json!({
        "messages": &request.messages,
        "origin": {"kind": "import", "source_ref": &request.origin.source_ref},
    }))
    .map_err(|error| ImportError::Wal(error.to_string()))?;
    let request_digest = format!("{:x}", Sha256::digest(identity_bytes));
    let (messages, counts) = normalize_messages(request.messages);
    // Normalization can empty a request that arrived non-empty: it drops system messages, which
    // belong to the continuing session rather than the transcript, and orphan tool results. A
    // system-only export reaches here with nothing left, and seeding an empty episode would
    // consume the lineage exactly as an empty request would -- so the same refusal applies, and
    // it must be checked after normalization rather than only at the edge.
    if messages.is_empty() {
        return Err(ImportError::NoImportableContent);
    }
    let origin = RunOrigin::SeededFrom {
        origin_kind: SeedOriginKind::Import,
        source_ref: request.origin.source_ref,
        origin_device_pseudonym: None,
        base_seq: None,
    };
    let frames = wal_store
        .read_records(&request.session)
        .map_err(|error| ImportError::Wal(error.to_string()))?;

    if let Some((durable_input, durable_origin, durable_config, import_terminal_seq)) =
        complete_import_for(&frames, &request.run_id)
    {
        let durable_digest = durable_config
            .provider_options
            .get("import_request_digest")
            .and_then(serde_json::Value::as_str);
        if durable_input == messages.as_slice()
            && durable_origin == &origin
            && durable_digest == Some(request_digest.as_str())
        {
            // The matching terminal may be a complete page-cache frame after a failed prior
            // barrier. Re-establish this writer's durable point before issuing an idempotent ack.
            let safe_h = writer
                .sync()
                .await
                .map_err(|error| ImportError::Wal(error.to_string()))?;
            if safe_h < import_terminal_seq {
                return Err(ImportError::Wal(format!(
                    "import terminal seq {import_terminal_seq} is beyond the writer's proven durable head {safe_h}"
                )));
            }
            if let Some(store) = store {
                crate::run::project_committed(wal_store, store, &request.session, safe_h, false)
                    .await;
            }
            return Ok(ImportOutcome {
                run_id: request.run_id,
                synthesized_tool_results: counts.synthesized_tool_results,
                dropped_orphan_tool_results: counts.dropped_orphan_tool_results,
                dropped_system_messages: counts.dropped_system_messages,
                already_imported: true,
            });
        }
        return Err(ImportError::ImportIdReuse);
    }

    if !frames.is_empty() {
        let dangling_seed =
            repairable_import_prefix(&frames)?.ok_or(ImportError::LineageNotEmpty)?;
        if dangling_seed {
            writer.record(WalRecord::RunFinished {
                ts_ms: epoch_millis(),
                reason: RunFinishReason::Interrupted,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
        }
    }

    let session = request.session.clone();
    writer.record(WalRecord::RunStarted {
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: Some(1),
        // An import fetches nothing from tool providers, so it has no plan manifest to freeze.
        manifest: None,
        // An import seeds a new, unscoped session; a scope is frozen only by a scoped send.
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: request.project_id.clone(),
        principal: request.principal.clone(),
        ts_ms: epoch_millis(),
        run_id: request.run_id.clone(),
        session: request.session,
        config: Box::new(import_marker_config(&request_digest)),
        input: messages.into(),
        origin,
        submission_id: None,
    });
    writer.record(WalRecord::RunFinished {
        ts_ms: epoch_millis(),
        reason: RunFinishReason::Completed,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    });
    let safe_h = writer
        .sync()
        .await
        .map_err(|error| ImportError::Wal(error.to_string()))?;

    // The WAL terminal barrier is the import's durability truth. Projection happens afterward
    // while the caller still holds the session lease, and any store failure stays non-fatal so
    // the acknowledged import remains readable through full-WAL replay.
    if let Some(store) = store {
        crate::run::project_committed(wal_store, store, &session, safe_h, false).await;
    }

    Ok(ImportOutcome {
        run_id: request.run_id,
        synthesized_tool_results: counts.synthesized_tool_results,
        dropped_orphan_tool_results: counts.dropped_orphan_tool_results,
        dropped_system_messages: counts.dropped_system_messages,
        already_imported: false,
    })
}

fn import_marker_config(request_digest: &str) -> RunConfig {
    RunConfig {
        model: ModelHandle::new("import", "import"),
        tools: Vec::new().into(),
        tool_choice: ToolChoice::None,
        generation: GenerationConfig::default(),
        response_format: ResponseFormat::default(),
        provider_options: serde_json::json!({"import_request_digest": request_digest}),
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 0,
        // Null is deliberate: the W3-B frozen-render guard refuses to resume this marker as
        // a provider run. Replay classifies a dangling zero-step SeededFrom episode as
        // Interrupted before any provider can be selected.
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        // An import seed chooses no context management: the first harness episode after it is
        // the session's first real admission, and binds the value then.
        context_management: None,
    }
}

fn repairable_import_prefix(
    frames: &[broca_wal::FramedRecord],
) -> Result<Option<bool>, ImportError> {
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let state = crate::replay(&records).map_err(|error| ImportError::Wal(error.to_string()))?;
    let mut dangling_seed = false;
    for record in &records {
        match record {
            WalRecord::RunStarted {
                origin:
                    RunOrigin::SeededFrom {
                        origin_kind: SeedOriginKind::Import,
                        ..
                    },
                ..
            } if !dangling_seed => dangling_seed = true,
            WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            } if dangling_seed => dangling_seed = false,
            // A record type from a newer build is informational by the contract on
            // `WalRecord`, so it neither makes nor breaks a repairable import prefix.
            WalRecord::Unknown => {}
            // This reader release decodes compaction records but does not act on them yet.
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. }
            | WalRecord::CompactionWait { .. } => {}
            // Provider attempt evidence is informational too, so it neither makes nor breaks a
            // repairable import prefix.
            WalRecord::ModelAttemptFinished { .. } => {}
            _ => return Ok(None),
        }
    }
    let replay_classifies_repairable = if dangling_seed {
        matches!(
            state.resolution,
            crate::ReplayResolution::Interrupted { .. }
        )
    } else {
        matches!(
            state.resolution,
            crate::ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Interrupted
            }
        )
    };
    Ok(replay_classifies_repairable.then_some(dangling_seed))
}

fn complete_import_for<'a>(
    frames: &'a [broca_wal::FramedRecord],
    run_id: &RunId,
) -> Option<(&'a [Message], &'a RunOrigin, &'a RunConfig, u64)> {
    let mut found = None;
    // Adjacency is judged over the records with session meaning: a record type from a newer
    // build, or attempt evidence, between the two is informational and must not hide the pair.
    let known: Vec<&broca_wal::FramedRecord> = frames
        .iter()
        .filter(|frame| !frame.record.is_informational())
        .collect();
    for pair in known.windows(2) {
        let WalRecord::RunStarted {
            run_id: durable_run_id,
            input,
            origin,
            config,
            ..
        } = &pair[0].record
        else {
            continue;
        };
        if durable_run_id != run_id || !matches!(origin, RunOrigin::SeededFrom { .. }) {
            continue;
        }
        if matches!(
            pair[1].record,
            WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ..
            }
        ) {
            found = Some((input.as_slice(), origin, config.as_ref(), pair[1].seq));
        }
    }
    found
}

/// Pending calls stay in source order while each id indexes its unmatched positions.
/// Consuming a result tombstones the earliest matching position without shifting later calls.
#[derive(Default)]
struct PendingToolCalls {
    ordered: Vec<Option<ToolCallId>>,
    positions: HashMap<ToolCallId, VecDeque<usize>>,
}

impl PendingToolCalls {
    fn push(&mut self, tool_call_id: ToolCallId) {
        let position = self.ordered.len();
        self.ordered.push(Some(tool_call_id.clone()));
        self.positions
            .entry(tool_call_id)
            .or_default()
            .push_back(position);
    }

    fn consume_first(&mut self, tool_call_id: &ToolCallId) -> bool {
        let (position, exhausted) = match self.positions.get_mut(tool_call_id) {
            Some(positions) => {
                let position = positions
                    .pop_front()
                    .expect("pending tool-call id keeps at least one position");
                (position, positions.is_empty())
            }
            None => return false,
        };
        if exhausted {
            self.positions.remove(tool_call_id);
        }
        let consumed = self.ordered[position]
            .take()
            .expect("pending tool-call position is consumed only once");
        debug_assert_eq!(&consumed, tool_call_id);
        true
    }

    fn drain_remaining(&mut self) -> impl Iterator<Item = ToolCallId> + '_ {
        self.positions.clear();
        self.ordered.drain(..).flatten()
    }
}

/// Counts of every rewrite normalization performs, so the outcome can report all of them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct NormalizationCounts {
    synthesized_tool_results: usize,
    dropped_orphan_tool_results: usize,
    dropped_system_messages: usize,
}

fn normalize_messages(messages: Vec<Message>) -> (Vec<Message>, NormalizationCounts) {
    let mut normalized = Vec::with_capacity(messages.len());
    let mut pending = PendingToolCalls::default();
    let mut counts = NormalizationCounts::default();

    for mut message in messages {
        if message.role == Role::System {
            // Dropped rather than carried: the continuing session owns the system prompt, and
            // an imported one would fail the integrity check on the next send.
            counts.dropped_system_messages += 1;
            continue;
        }
        if message.role == Role::User {
            // Anthropic carries results in user turns. Pair them before treating
            // the remaining user content as an interruption, and store them in
            // a tool turn so every renderer sees the canonical role.
            let mut results = Vec::new();
            message.content.retain(|block| {
                if let ContentBlock::ToolResult(result) = block {
                    if pending.consume_first(&result.tool_call_id) {
                        results.push(block.clone());
                    } else {
                        counts.dropped_orphan_tool_results += 1;
                    }
                    false
                } else {
                    true
                }
            });
            if !results.is_empty() {
                normalized.push(Message::new(Role::Tool, results));
            }
        }
        if message.role != Role::Tool {
            flush_pending(
                &mut normalized,
                &mut pending,
                &mut counts.synthesized_tool_results,
            );
        }

        message.content.retain(|block| match block {
            ContentBlock::ToolResult(result) => {
                if pending.consume_first(&result.tool_call_id) {
                    true
                } else {
                    counts.dropped_orphan_tool_results += 1;
                    false
                }
            }
            _ => true,
        });
        for block in &message.content {
            if let ContentBlock::ToolCall(call) = block {
                pending.push(call.tool_call_id.clone());
            }
        }
        if !message.content.is_empty() {
            normalized.push(message);
        }
    }
    flush_pending(
        &mut normalized,
        &mut pending,
        &mut counts.synthesized_tool_results,
    );
    (normalized, counts)
}

fn flush_pending(
    messages: &mut Vec<Message>,
    pending: &mut PendingToolCalls,
    synthesized: &mut usize,
) {
    let content: Vec<_> = pending
        .drain_remaining()
        .map(|tool_call_id| {
            *synthesized += 1;
            ContentBlock::ToolResult(ToolResult {
                tool_call_id,
                output: ToolOutput::Error {
                    message: "tool not run: interrupted before dispatch".into(),
                },
                is_error: true,
            })
        })
        .collect();
    if !content.is_empty() {
        messages.push(Message::new(Role::Tool, content));
    }
}

#[cfg(test)]
mod tests {
    use broca_protocol::content::ToolCall;
    use serde_json::json;

    use super::*;

    fn tool_call(id: &str) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(id.into()),
            tool_name: "test_tool".into(),
            input: json!({}),
            thought_signature: None,
        })
    }

    fn tool_result(id: &str, label: &str) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: label.into() },
            is_error: false,
        })
    }

    #[test]
    fn anthropic_user_results_are_consumed_before_interrupting_pending_calls() {
        let calls = Message::new(Role::Assistant, vec![tool_call("a"), tool_call("b")]);
        let text = ContentBlock::Text {
            text: "continue".into(),
        };
        let (messages, counts) = normalize_messages(vec![
            calls.clone(),
            Message::new(
                Role::User,
                vec![
                    tool_result("a", "real output"),
                    text.clone(),
                    tool_result("orphan", "discard"),
                ],
            ),
        ]);
        assert_eq!(counts.synthesized_tool_results, 1);
        assert_eq!(counts.dropped_orphan_tool_results, 1);
        assert_eq!(messages[0], calls);
        assert_eq!(
            messages[1],
            Message::new(Role::Tool, vec![tool_result("a", "real output")])
        );
        assert_eq!(synthesized_result_ids(&messages), vec!["b"]);
        assert_eq!(
            messages.last().unwrap(),
            &Message::new(Role::User, vec![text])
        );
        let (results_only, counts) = normalize_messages(vec![
            Message::new(Role::Assistant, vec![tool_call("a")]),
            Message::new(Role::User, vec![tool_result("a", "real output")]),
        ]);
        assert_eq!(counts, NormalizationCounts::default());
        assert_eq!(results_only.len(), 2);
        assert_eq!(results_only[1].role, Role::Tool);
    }

    /// Every rewrite normalization performs has to reach the caller. Dropping system
    /// messages is correct, but a caller who cannot see it happened reads the same report
    /// they would get for a transcript that never contained one.
    #[test]
    fn dropped_system_messages_are_counted_rather_than_silently_discarded() {
        let input = vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "you are a bot".into(),
                }],
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]),
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "and be brief".into(),
                }],
            ),
        ];

        let (normalized, counts) = normalize_messages(input);

        assert_eq!(
            counts.dropped_system_messages, 2,
            "both system messages must be counted, not just the leading one"
        );
        assert!(
            !normalized.iter().any(|m| m.role == Role::System),
            "the drop itself is the established behaviour and must not change"
        );
        // A transcript with nothing to drop must report zero, so a non-zero count always
        // means something was actually removed.
        let (_, clean) = normalize_messages(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "hi".into() }],
        )]);
        assert_eq!(clean.dropped_system_messages, 0);
    }

    fn pairing_input(call_ids: &[String], result_ids: &[String]) -> Vec<Message> {
        vec![
            Message::new(
                Role::Assistant,
                call_ids.iter().map(|id| tool_call(id)).collect(),
            ),
            Message::new(
                Role::Tool,
                result_ids
                    .iter()
                    .enumerate()
                    .map(|(index, id)| tool_result(id, &format!("result-{index}")))
                    .collect(),
            ),
        ]
    }

    fn retained_result_labels(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResult(ToolResult {
                    output: ToolOutput::Text { text },
                    ..
                }) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    fn synthesized_result_ids(messages: &[Message]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id,
                    output: ToolOutput::Error { message },
                    is_error: true,
                }) if message == "tool not run: interrupted before dispatch" => {
                    Some(tool_call_id.0.clone())
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn tool_result_pairing_preserves_order_multiplicity_and_orphan_rules() {
        struct Case {
            name: &'static str,
            call_ids: Vec<String>,
            result_ids: Vec<String>,
            retained_result_indices: Vec<usize>,
            synthesized_ids: Vec<String>,
            dropped: usize,
        }

        let wide_ids: Vec<String> = (0..1_024).map(|index| format!("call-{index}")).collect();
        let mut reverse_ids = wide_ids.clone();
        reverse_ids.reverse();
        let cases = vec![
            Case {
                name: "source ordered",
                call_ids: wide_ids.clone(),
                result_ids: wide_ids.clone(),
                retained_result_indices: (0..wide_ids.len()).collect(),
                synthesized_ids: Vec::new(),
                dropped: 0,
            },
            Case {
                name: "reverse ordered",
                call_ids: wide_ids,
                result_ids: reverse_ids.clone(),
                retained_result_indices: (0..reverse_ids.len()).collect(),
                synthesized_ids: Vec::new(),
                dropped: 0,
            },
            Case {
                name: "duplicate id consumes only matching call multiplicity",
                call_ids: vec!["same".into(), "same".into(), "same".into()],
                result_ids: vec!["same".into(), "same".into(), "same".into(), "same".into()],
                retained_result_indices: vec![0, 1, 2],
                synthesized_ids: Vec::new(),
                dropped: 1,
            },
            Case {
                name: "dangling calls synthesize in original call order",
                call_ids: vec!["a".into(), "b".into(), "a".into(), "c".into()],
                result_ids: vec!["a".into()],
                retained_result_indices: vec![0],
                synthesized_ids: vec!["b".into(), "a".into(), "c".into()],
                dropped: 0,
            },
            Case {
                name: "orphan result is dropped",
                call_ids: vec!["a".into()],
                result_ids: vec!["orphan".into(), "a".into()],
                retained_result_indices: vec![1],
                synthesized_ids: Vec::new(),
                dropped: 1,
            },
        ];

        for case in cases {
            let input = pairing_input(&case.call_ids, &case.result_ids);
            let (normalized, counts) = normalize_messages(input);
            let expected_labels: Vec<String> = case
                .retained_result_indices
                .iter()
                .map(|index| format!("result-{index}"))
                .collect();
            assert_eq!(
                retained_result_labels(&normalized),
                expected_labels,
                "{} retained results",
                case.name
            );
            assert_eq!(
                synthesized_result_ids(&normalized),
                case.synthesized_ids,
                "{} synthesized order",
                case.name
            );
            assert_eq!(
                counts.synthesized_tool_results,
                case.synthesized_ids.len(),
                "{}",
                case.name
            );
            assert_eq!(
                counts.dropped_orphan_tool_results, case.dropped,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn fixed_multi_batch_normalization_matches_byte_stable_golden() {
        let input = vec![
            Message::new(Role::Assistant, vec![tool_call("a"), tool_call("b")]),
            Message::new(
                Role::Tool,
                vec![tool_result("b", "result-b"), tool_call("d")],
            ),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "continue".into(),
                }],
            ),
            Message::new(Role::Assistant, vec![tool_call("c")]),
            Message::new(
                Role::Tool,
                vec![
                    tool_result("orphan", "drop-me"),
                    tool_result("c", "result-c"),
                ],
            ),
        ];
        let golden = vec![
            Message::new(Role::Assistant, vec![tool_call("a"), tool_call("b")]),
            Message::new(
                Role::Tool,
                vec![tool_result("b", "result-b"), tool_call("d")],
            ),
            Message::new(
                Role::Tool,
                vec![
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("a".into()),
                        output: ToolOutput::Error {
                            message: "tool not run: interrupted before dispatch".into(),
                        },
                        is_error: true,
                    }),
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("d".into()),
                        output: ToolOutput::Error {
                            message: "tool not run: interrupted before dispatch".into(),
                        },
                        is_error: true,
                    }),
                ],
            ),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "continue".into(),
                }],
            ),
            Message::new(Role::Assistant, vec![tool_call("c")]),
            Message::new(Role::Tool, vec![tool_result("c", "result-c")]),
        ];

        let (normalized, counts) = normalize_messages(input);
        assert_eq!(counts.synthesized_tool_results, 2);
        assert_eq!(counts.dropped_orphan_tool_results, 1);
        assert_eq!(normalized, golden);
        assert_eq!(
            serde_json::to_vec(&normalized).unwrap(),
            serde_json::to_vec(&golden).unwrap(),
            "normalized WAL input bytes must remain stable"
        );
    }
}

#[cfg(test)]
mod durability_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use broca_protocol::BindIdentity;
    use broca_wal::{FramedRecord, Wal, WalError};

    use super::*;

    #[derive(Default)]
    struct PoisoningState {
        records: Vec<FramedRecord>,
    }

    struct PoisoningWal {
        state: Arc<Mutex<PoisoningState>>,
        sync_calls: Arc<AtomicUsize>,
    }

    impl Wal for PoisoningWal {
        fn append(
            &mut self,
            _fence: u64,
            record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, WalError> {
            let mut state = self.state.lock().unwrap();
            let seq = state.records.last().map_or(1, |frame| frame.seq + 1);
            state.records.push(FramedRecord {
                seq,
                fence: 1,
                record: record.clone(),
            });
            Ok(seq)
        }

        fn sync(&self) -> Result<(), WalError> {
            if self.sync_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // The first sync after opening proves the recovered complete prefix durable
                // before the handle exposes its durable head.
                Ok(())
            } else {
                Err(WalError::Io(std::io::Error::other(
                    "injected import barrier failure",
                )))
            }
        }

        fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
            Ok(self.state.lock().unwrap().records.clone())
        }

        fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .records
                .iter()
                .filter(|frame| frame.seq > after_seq)
                .cloned()
                .collect())
        }

        fn last_seq(&self) -> u64 {
            self.state
                .lock()
                .unwrap()
                .records
                .last()
                .map_or(0, |frame| frame.seq)
        }
    }

    struct PoisoningStore {
        state: Arc<Mutex<PoisoningState>>,
    }

    impl WalStore for PoisoningStore {
        fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            unreachable!("the test supplies its writer directly")
        }

        fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
            Ok(self.state.lock().unwrap().records.clone())
        }
    }

    #[tokio::test]
    async fn poisoned_import_retry_never_acknowledges_complete_but_unproven_frames() {
        let state = Arc::new(Mutex::new(PoisoningState::default()));
        let sync_calls = Arc::new(AtomicUsize::new(0));
        let writer = WalHandle::spawn(
            Box::new(PoisoningWal {
                state: state.clone(),
                sync_calls,
            }),
            1,
        )
        .expect("initial recovery proof succeeds");
        let store = PoisoningStore {
            state: state.clone(),
        };
        let request = ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("import-poisoned-retry".into()),
            session: BindIdentity::new("/tmp/import", "runner", "session"),
            messages: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "must not be acknowledged without a barrier".into(),
                }],
            )],
            origin: ImportOrigin {
                source_ref: "fixture:poisoned-retry".into(),
            },
        };

        assert!(
            run_import(request.clone(), &writer, &store).await.is_err(),
            "the injected terminal barrier fails after the complete frames are appended"
        );
        assert_eq!(state.lock().unwrap().records.len(), 2);
        assert!(writer.is_poisoned());

        let retry = run_import(request, &writer, &store)
            .await
            .expect_err("a matching complete tail must not receive already_imported after poison");
        assert!(matches!(retry, ImportError::Wal(_)));
        writer.shutdown().await;

        // The Active-path preflight receives a distinct writer while the complete matching frames
        // are readable from a store. Its durable head is zero, so it must refuse the shortcut.
        let fresh_writer = WalHandle::spawn(
            Box::new(PoisoningWal {
                state: Arc::new(Mutex::new(PoisoningState::default())),
                sync_calls: Arc::new(AtomicUsize::new(0)),
            }),
            2,
        )
        .expect("empty writer recovery succeeds");
        let preflight = check_import_retry(
            &ImportRequest {
                project_id: None,
                principal: None,
                run_id: RunId("import-poisoned-retry".into()),
                session: BindIdentity::new("/tmp/import", "runner", "session"),
                messages: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "must not be acknowledged without a barrier".into(),
                    }],
                )],
                origin: ImportOrigin {
                    source_ref: "fixture:poisoned-retry".into(),
                },
            },
            &store,
            &fresh_writer,
        );
        assert!(matches!(preflight, Err(ImportError::Wal(_))));
        fresh_writer.shutdown().await;
    }
}
