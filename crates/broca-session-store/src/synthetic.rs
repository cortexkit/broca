//! A synthetic session shaped like real ones, for the memory budget tests.
//!
//! The shape follows the production sessions the memory design measured: per 1,000 window
//! messages, about 525 tool calls and 6,150 records, 4.5 KB of content per message on average
//! with tool results carrying most of it, a schema pin on every call, JSON tool inputs of a few
//! keys, and several runs, so the latest run's records are a fraction of the open log. Every
//! size is a field of [`Shape`]. Every name and every text is made up.
//!
//! The records are the ones the run loop writes for a step, batch by batch: a step's start, its
//! attempt, the assistant message with the step's end and the message's facts, then for a step
//! that made calls the batch's start with each call's intent, each call's reply and outcome,
//! and the batch's end with the results message's facts and the turn's call facts.
//!
//! Built for the crate's tests and, behind the `synthetic` feature, for other crates' tests.
//! Never for a shipped binary.

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{
    ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;

use crate::backend::SessionBackend;
use crate::codec::Digest;
use crate::fold::conversation::Conversation;
use crate::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallFacts, CallOutcome, EntryFacts, Input, Placement,
    Record, RunEnd, RunStart, StepEnd, StepStart, StepUsage, ToolIntent, ToolOutcome, ToolReply,
    TurnFacts,
};
use crate::session::{Session, SessionError};

/// The shape of a synthetic session. [`Shape::production`] gives the measured one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    /// Window messages to write, about: the session is whole runs, so it can run a few over.
    pub messages: usize,
    /// Runs the window holds. The latest run holds about `messages / runs` of them.
    pub runs: usize,
    /// Tool calls per 1,000 messages. A step makes one call, or two where the count needs more.
    pub calls_per_1000: usize,
    /// Bytes of text in one tool result.
    pub tool_result_bytes: usize,
    /// Bytes of text in one assistant message, besides its calls.
    pub assistant_text_bytes: usize,
    /// Bytes of text in one run's input.
    pub input_text_bytes: usize,
    /// Keys in one tool call's JSON input.
    pub input_keys: usize,
}

impl Shape {
    /// The shape the memory design measured on production sessions, at `messages` window
    /// messages: 4.5 KB per message, 525 calls per 1,000 messages, six runs.
    pub fn production(messages: usize) -> Self {
        Self {
            messages,
            runs: 6,
            calls_per_1000: 525,
            tool_result_bytes: 7_600,
            assistant_text_bytes: 900,
            input_text_bytes: 2_000,
            input_keys: 3,
        }
    }
}

/// What [`write`] wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    /// Window messages placed.
    pub messages: usize,
    /// Tool calls made.
    pub calls: usize,
    /// Records appended.
    pub records: usize,
    /// Records of the latest run, from its start on.
    pub latest_run_records: usize,
}

/// The model every synthetic run names.
pub fn model() -> ModelHandle {
    ModelHandle::new("synthetic-provider", "synthetic-model")
}

/// The window's messages as compact JSON, the `W` the memory budgets are written in.
pub fn window_bytes(conversation: &Conversation) -> usize {
    // Synthetic sessions release nothing, so every message is held.
    let messages = conversation
        .held_messages()
        .expect("a synthetic window is held");
    serde_json::to_vec(&messages).map_or(0, |bytes| bytes.len())
}

/// Appends a session of `shape` to `session`, which holds no run yet, and syncs it.
pub async fn write<B: SessionBackend>(
    session: &mut Session<B>,
    shape: &Shape,
) -> Result<Written, SessionError> {
    let mut writer = Writer {
        text: Text::new(0x5eed),
        ordinal: session.conversation.placed,
        written: Written::default(),
        shape: shape.clone(),
    };
    let runs = shape.runs.max(1);
    for run in 0..runs {
        // Each run takes its share of the messages still to write, so the window reaches
        // `messages` and the runs are about even.
        let left = shape.messages.saturating_sub(writer.written.messages);
        let per_run = left.div_ceil(runs - run).max(4);
        let latest_from = writer.written.records;
        for batch in writer.run(run, per_run) {
            writer.written.records += batch.len();
            session.append(&batch, false).await?;
        }
        writer.written.latest_run_records = writer.written.records - latest_from;
    }
    session.sync().await?;
    Ok(writer.written)
}

/// Deterministic made-up text: words of a small invented vocabulary, picked by a linear
/// congruential sequence.
struct Text {
    state: u64,
}

const WORDS: [&str; 16] = [
    "lorem", "vexa", "tumbral", "quoth", "nerilo", "sabent", "orvic", "palune", "dremm", "kostra",
    "ilv", "zephet", "marrow", "ubique", "trisk", "fennow",
];

impl Text {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state >> 33
    }

    /// About `bytes` bytes of words, with a line break now and then.
    fn words(&mut self, bytes: usize) -> String {
        let mut out = String::with_capacity(bytes + 16);
        while out.len() < bytes {
            let n = self.next();
            out.push_str(WORDS[(n % 16) as usize]);
            out.push(if n.is_multiple_of(11) { '\n' } else { ' ' });
        }
        out.truncate(bytes);
        out.shrink_to_fit();
        out
    }

    fn hex(&mut self, len: usize) -> String {
        let mut out = String::with_capacity(len);
        while out.len() < len {
            out.push_str(&format!("{:08x}", self.next() as u32));
        }
        out.truncate(len);
        out
    }
}

struct Writer {
    text: Text,
    /// The last ordinal placed.
    ordinal: u64,
    written: Written,
    shape: Shape,
}

impl Writer {
    /// The batches of one run of about `messages` messages: its start and input, tool steps, and
    /// a final answer.
    fn run(&mut self, run: usize, messages: usize) -> Vec<Vec<Record>> {
        let run_id = format!("run-{run:04}");
        let mut batches = vec![vec![run_start(&run_id)]];
        let input = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: self.text.words(self.shape.input_text_bytes).into(),
            }],
        );
        self.ordinal += 1;
        batches.push(vec![
            Record::Input(Input {
                media: Vec::new(),
                run_id: run_id.clone(),
                placement: Placement::RunInput,
                message: input.into(),
                written_at_ms: Some(self.now()),
                submission_id: Some(format!("sub-{run:04}")),
                notice: None,
                tags_v1: None,
                receipt_seq: None,
                pruned: Vec::new(),
            }),
            Record::EntryFacts(self.facts(&run_id, false).into()),
        ]);
        self.written.messages += 1;
        // Tool steps take two messages each; the input and the answer take the other two.
        let steps = messages.saturating_sub(2).div_ceil(2);
        let calls = (messages * self.shape.calls_per_1000)
            .div_ceil(1000)
            .max(steps);
        let doubled = calls - steps;
        for step in 0..steps {
            // The steps that make two calls, spread over the run.
            let two = doubled > 0 && (step * doubled) / steps != ((step + 1) * doubled) / steps;
            batches.extend(self.tool_step(&run_id, step as u64 + 1, if two { 2 } else { 1 }));
        }
        batches.extend(self.answer_step(&run_id, steps as u64 + 1));
        batches.push(vec![Record::RunEnd(RunEnd {
            run_id,
            reason: RunFinishReason::Completed,
            usage: Some(usage()),
            ts_ms: Some(self.now()),
            retries_used: 0,
            interrupt_reason: None,
            error: None,
        })]);
        batches
    }

    /// A clock that moves a little with every message.
    fn now(&self) -> u64 {
        1_700_000_000_000 + self.ordinal * 1_500
    }

    fn mid(ordinal: u64) -> String {
        format!("m-syn-{ordinal:08}")
    }

    /// Facts of the message at the current ordinal.
    fn facts(&self, run_id: &str, native: bool) -> EntryFacts {
        EntryFacts {
            ordinal: self.ordinal,
            written_at_ms: Some(self.now()),
            tag_base: self.ordinal * 2,
            notice: None,
            usage: native.then_some(StepUsage {
                prompt_tokens: Some(40_000 + self.ordinal),
                cache_read_tokens: Some(38_000),
                cache_write_tokens: Some(1_200),
                output_tokens: Some(300),
                reasoning_tokens: None,
            }),
            generated_under: None,
            native,
            render_tags: true,
            clean_reply: true,
            mid: Self::mid(self.ordinal),
            run_id: Some(run_id.into()),
            created_at_ms: Some(self.now()),
            delivery_mode: None,
            delivery_tool_call_id: None,
            client_message_id: None,
            sender: Some("synthetic.caller".into()),
            marks: None,
            no_display_facts: None,
            reasoning_summaries: Vec::new(),
            rich_ui: None,
            runner_tag_base: Some(self.ordinal * 2),
            legacy_view_seq: None,
            notices: Vec::new(),
            named_speaker: None,
            kept_notice: None,
            send_id: None,
        }
    }

    fn attempt(&mut self, run_id: &str, step_id: u64) -> Record {
        Record::Attempt(Attempt {
            run_id: run_id.to_owned(),
            step_id,
            attempt: 1,
            attempt_id: Some(self.text.hex(32)),
            outcome: "ok".into(),
            http_status: Some(200),
            request_body_len: 180_000,
            request_body_sha256: Digest([7; 32]),
            response_body_len: 4_000,
            response_body_sha256: Digest([9; 32]),
            ts_ms: self.now(),
            sent_at_ms: Some(self.now()),
            first_content_at_ms: Some(self.now() + 400),
            last_content_at_ms: Some(self.now() + 900),
            transport: Some("https".into()),
            served_account_ref: None,
            billing: None,
            telemetry: None,
            reached_cache: Some(true),
            requested_cache_ttl_ms: None,
            tools_digest: None,
            system_digest: None,
            signed_floor: None,
        })
    }

    fn step_end(run_id: &str, step_id: u64, calls: u32, finish: FinishReason) -> Record {
        Record::StepEnd(StepEnd {
            run_id: run_id.to_owned(),
            step_id,
            usage: Some(usage()),
            finish_reason: Some(finish),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            call_count: calls,
            fallback: None,
            hooks: None,
            budget_finalize: None,
            total_usage: None,
            stop_met: None,
            limiting_stop_met: None,
            by_model: Vec::new(),
        })
    }

    fn assistant(&self, run_id: &str, step_id: u64, content: Vec<ContentBlock>) -> Record {
        Record::Assistant(Assistant {
            run_id: run_id.to_owned(),
            step_id,
            provider_message_id: MessageId(format!("prov-{run_id}-{step_id}")),
            message: Message::assistant_from(content, model()).into(),
            usage: Some(usage()),
            written_at_ms: Some(self.now()),
            last_content_at_ms: Some(self.now()),
            budget_finalize: None,
            tags_v1: None,
            pruned: Vec::new(),
        })
    }

    /// A JSON tool input of a few keys.
    fn tool_input(&mut self, call: usize) -> serde_json::Value {
        let mut input = serde_json::Map::new();
        input.insert(
            "command".into(),
            serde_json::Value::String(self.text.words(90)),
        );
        if self.shape.input_keys > 1 {
            input.insert(
                "description".into(),
                serde_json::Value::String(self.text.words(40)),
            );
        }
        for key in 2..self.shape.input_keys {
            input.insert(format!("option_{key}"), serde_json::json!(call * key));
        }
        serde_json::Value::Object(input)
    }

    fn schema_pin(tool: &str) -> serde_json::Value {
        serde_json::json!({
            "tool": tool,
            "module": "synthetic.tools",
            "digest": "ab".repeat(32),
        })
    }

    /// A step that makes `calls` calls, and the batch that answers them.
    fn tool_step(&mut self, run_id: &str, step_id: u64, calls: usize) -> Vec<Vec<Record>> {
        let assistant_ordinal = self.ordinal + 1;
        let results_ordinal = self.ordinal + 2;
        let batch_id = format!("batch-{run_id}-{step_id}");
        let tools: Vec<(ToolCallId, String, serde_json::Value)> = (0..calls)
            .map(|n| {
                (
                    ToolCallId(format!("call-{run_id}-{step_id}-{n}")),
                    if n == 0 { "shell" } else { "read_file" }.to_owned(),
                    self.tool_input(n),
                )
            })
            .collect();
        let mut content = vec![ContentBlock::Text {
            text: self.text.words(self.shape.assistant_text_bytes).into(),
        }];
        content.extend(tools.iter().map(|(id, name, input)| {
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: id.clone(),
                tool_name: name.clone(),
                input: input.clone().into(),
                thought_signature: None,
                provider_state: None,
            })
        }));
        let step_start = Record::StepStart(StepStart {
            run_id: run_id.to_owned(),
            step_id,
            mid: Some(Self::mid(assistant_ordinal)),
        });
        let attempt = self.attempt(run_id, step_id);
        let assistant = self.assistant(run_id, step_id, content);
        let end = Self::step_end(run_id, step_id, calls as u32, FinishReason::ToolCalls);
        // The assistant message is held for its results at its reserved ordinal.
        self.ordinal = assistant_ordinal;
        let assistant_facts = Record::EntryFacts(self.facts(run_id, true).into());
        self.ordinal = assistant_ordinal - 1;
        let mut dispatch = vec![Record::BatchStart(BatchStart {
            run_id: run_id.to_owned(),
            batch_id: batch_id.clone(),
            step_id,
            planned: tools.iter().map(|(id, ..)| id.clone()).collect(),
            mid: Some(Self::mid(results_ordinal)),
            shown_tags: Vec::new(),
        })];
        let mut answers = Vec::new();
        let mut call_facts = Vec::new();
        for (id, name, input) in &tools {
            let key = format!("key-{}", self.text.hex(24));
            dispatch.push(Record::ToolIntent(ToolIntent {
                run_id: run_id.to_owned(),
                batch_id: batch_id.clone(),
                tool_call_id: id.clone(),
                tool_name: Some(name.clone()),
                module_id: Some("synthetic.tools".into()),
                args_digest: Digest([3; 32]),
                args: Some(input.clone().into()),
                call_key: Some(key.clone()),
                approval_digest: None,
                dispatched_at_ms: Some(self.now()),
                schema_pin: Some(Self::schema_pin(name).into()),
                hooks: None,
                key_derived: None,
            }));
            answers.push(Record::ToolReply(ToolReply {
                run_id: run_id.to_owned(),
                batch_id: Some(batch_id.clone()),
                result: ToolResult {
                    tool_call_id: id.clone(),
                    output: ToolOutput::Text {
                        text: self.text.words(self.shape.tool_result_bytes / calls).into(),
                    },
                    is_error: false,
                },
                finished_at_ms: Some(self.now() + 700),
                tags_v1: None,
                media: Vec::new(),
            }));
            answers.push(Record::ToolOutcome(ToolOutcome {
                yielded_by: None,
                run_id: run_id.to_owned(),
                batch_id: batch_id.clone(),
                tool_call_id: id.clone(),
                outcome: CallOutcome::Completed,
                result_digest: Digest([5; 32]),
                is_error: false,
                reason: None,
                finished_at_ms: Some(self.now() + 700),
                detail: None,
                hooks: None,
                call_key: None,
            }));
            call_facts.push(CallFacts {
                tool_call_id: id.clone(),
                call_key: key,
                schema_pin: Some(Self::schema_pin(name).into()),
                carrier: None,
                dispatched_to: Some("synthetic.tools".into()),
                dispatched_at_ms: Some(self.now()),
                finished_at_ms: Some(self.now() + 700),
                unanswered: None,
            });
        }
        self.ordinal = results_ordinal;
        answers.push(Record::BatchEnd(BatchEnd {
            run_id: run_id.to_owned(),
            batch_id,
        }));
        answers.push(Record::EntryFacts(self.facts(run_id, true).into()));
        answers.push(Record::TurnFacts(
            TurnFacts {
                ordinal: assistant_ordinal,
                calls: call_facts,
            }
            .into(),
        ));
        self.written.messages += 2;
        self.written.calls += calls;
        vec![
            vec![step_start, attempt, assistant, end, assistant_facts],
            dispatch,
            answers,
        ]
    }

    /// A step that answers with text only.
    fn answer_step(&mut self, run_id: &str, step_id: u64) -> Vec<Vec<Record>> {
        let ordinal = self.ordinal + 1;
        let step_start = Record::StepStart(StepStart {
            run_id: run_id.to_owned(),
            step_id,
            mid: Some(Self::mid(ordinal)),
        });
        let attempt = self.attempt(run_id, step_id);
        let text = self.text.words(self.shape.assistant_text_bytes * 2);
        let assistant = self.assistant(
            run_id,
            step_id,
            vec![ContentBlock::Text { text: text.into() }],
        );
        self.ordinal = ordinal;
        let facts = Record::EntryFacts(self.facts(run_id, true).into());
        self.written.messages += 1;
        vec![vec![
            step_start,
            attempt,
            assistant,
            Self::step_end(run_id, step_id, 0, FinishReason::Stop),
            facts,
        ]]
    }
}

fn usage() -> Usage {
    Usage {
        input_tokens: Some(2_000),
        cached_input_tokens: Some(38_000),
        cache_write_tokens: Some(1_200),
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        output_tokens: Some(300),
        reasoning_tokens: None,
    }
}

fn run_start(run_id: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        kept_notice: None,
        imported_ids: None,
        message_marks: None,
        run_id: run_id.to_owned(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

#[cfg(test)]
mod tests;
