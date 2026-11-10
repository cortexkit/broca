//! Where a request stops sending kept signed reasoning because a run's tools changed.
//!
//! Anthropic binds signed thinking to the request's tool set as well as to the bytes before it.
//! Adding, removing or renaming a tool, or changing a tool's description or input schema,
//! invalidates every signed thinking block sent before that request, and the model provider
//! refuses a later request that still carries one. Only the set of tools counts: their order,
//! the tool choice and cache markers don't. A run freezes its tools when it starts, so the set
//! can only change where a run starts. The floor is the first message of the newest run whose
//! tool set differs from the run before it; a route whose reasoning is bound to its prefix sends
//! no signed reasoning from any message before it (`RenderHistory::signed_floor`), through the
//! same strip that drops reasoning before a compaction view.
//!
//! Everything here is computed from each run's start record (its frozen tools and render
//! config), so a resumed run and a request rebuilt offline find the floor the live run found.
//! Both run loops fold their run starts through [`SignedFloor::observe_run`], and the floor is
//! read from [`SignedFloor::floor`] alone.

use broca_protocol::request::Tool;
use broca_protocol::structured_output::{
    FrozenStructuredOutput, Mode, SUBMIT_RESULT_DESCRIPTION, SUBMIT_RESULT_TOOL,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The SHA-256 (lowercase hex) of the tools a run offers the model, as the model provider sees
/// them: each tool's model-visible name, description and input schema, as a set.
///
/// The set is sorted, so the order the caller declared the tools in (which the request keeps)
/// doesn't count, and a duplicate counts once. Broca-side metadata (the tool's module, execution
/// mode and reply deadline) is never sent, so it doesn't count either. From the run's frozen
/// render config come the two things that change what the provider sees: an operator alias
/// renames a tool on the wire, and a run admitted with a `submit_tool` structured output offers
/// the `submit_result` tool besides its own. Each tool is compared by its canonical JSON (RFC
/// 8785), so two storages that hold the same schema with its keys in another order agree.
pub fn tool_set_digest(tools: &[Tool], frozen_render_config: &Value) -> String {
    use sha2::Digest as _;
    let aliases = frozen_render_config
        .get("tool_name_aliases")
        .and_then(Value::as_object);
    let wire_name = |name: &str| {
        aliases
            .and_then(|aliases| aliases.get(name))
            .and_then(Value::as_str)
            .unwrap_or(name)
            .to_owned()
    };
    let submit = FrozenStructuredOutput::from_frozen_render_config(Some(frozen_render_config))
        .filter(|frozen| frozen.mode == Mode::SubmitTool)
        .map(|frozen| {
            (
                SUBMIT_RESULT_TOOL,
                SUBMIT_RESULT_DESCRIPTION,
                frozen.schema(),
            )
        });
    let shown = tools
        .iter()
        .map(|tool| {
            (
                tool.name.as_str(),
                tool.description.as_str(),
                &tool.input_schema,
            )
        })
        .chain(
            submit
                .as_ref()
                .map(|(name, description, schema)| (*name, *description, schema)),
        )
        .map(|(name, description, schema)| {
            serde_jcs::to_string(&serde_json::json!({
                "name": wire_name(name),
                "description": description,
                "input_schema": schema,
            }))
            .expect("a tool declaration is JSON")
        })
        .collect::<std::collections::BTreeSet<_>>();
    let bytes = serde_json::to_vec(&shown).expect("strings serialize");
    format!("{:x}", sha2::Sha256::digest(bytes))
}

/// The floor reached by folding a lineage's run starts in order, from its first run on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SignedFloor {
    /// The tool-set digest of the newest run folded; absent before the first.
    last: Option<String>,
    /// See [`SignedFloor::floor`].
    floor: Option<u64>,
}

impl SignedFloor {
    /// A fold that continues after runs it didn't see, which an admission checkpoint proves never
    /// changed their tool set: no floor yet, and the newest run offered the set `last`.
    pub fn unchanged(last: Option<String>) -> Self {
        Self { last, floor: None }
    }

    /// Fold the lineage's next run: `start` is the ordinal of its first input message and
    /// `digest` its [`tool_set_digest`]. A run that differs from the one before it moves the
    /// floor to its start. Runs start in increasing order, so the floor only moves forward; a
    /// later run that brings back an earlier set moves it again rather than restoring the
    /// reasoning the change already dropped, which requests since then no longer carry.
    pub fn observe_run(&mut self, start: u64, digest: String) {
        if self.last.as_ref().is_some_and(|last| *last != digest) {
            self.floor = Some(self.floor.map_or(start, |floor| floor.max(start)));
        }
        self.last = Some(digest);
    }

    /// The ordinal of the first message whose signed reasoning a request may still send, or
    /// `None` when no run changed the tool set. Every message before it loses its signed
    /// reasoning on a route that binds reasoning to its prefix.
    ///
    /// This is the one place the floor is read: requests carry it as
    /// `RenderHistory::signed_floor`, and a compaction notice that reports the floor reads it
    /// here. Ordinals count from 0, so in the 1-based entry numbering of a session the same
    /// number names the last entry whose signed parts are dropped.
    pub fn floor(&self) -> Option<u64> {
        self.floor
    }
}

/// What a replay or a run knows about its lineage's floor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FloorFold {
    /// The fold didn't start at the lineage's first run (a replay of a WAL tail), so the tool
    /// sets of earlier runs are unknown. Never read as "no floor": a run on a route that binds
    /// reasoning to its prefix replays the whole WAL instead.
    #[default]
    Unknown,
    Known(SignedFloor),
}

impl FloorFold {
    /// Fold the next run, when the earlier ones are known.
    pub fn observe_run(&mut self, start: u64, digest: String) {
        if let Self::Known(floor) = self {
            floor.observe_run(start, digest);
        }
    }

    /// The floor, when the fold knows it.
    pub fn known(&self) -> Option<&SignedFloor> {
        match self {
            Self::Unknown => None,
            Self::Known(floor) => Some(floor),
        }
    }
}

/// The tool-set facts an admission checkpoint folds from its lineage's run starts, so an append
/// admitted from the checkpoint knows the floor without replaying the WAL. The checkpoint counts
/// no transcript messages, so it can't hold the floor's ordinal. It holds the newest run's set
/// and whether any run ever changed it, which is all an unchanged lineage needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSetHistoryV1 {
    /// The newest run's [`tool_set_digest`]; absent before the first run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
    /// Whether any run's tool set differed from the run before it. Once set it stays set.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub changed: bool,
}

/// An admission checkpoint's [`ToolSetHistoryV1`], or the fact that it has none.
///
/// Stored as the history itself, and omitted when unknown, so a checkpoint written before this
/// member existed reads as [`ToolSetHistory::Unknown`], never as an unchanged history.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Option<ToolSetHistoryV1>", into = "Option<ToolSetHistoryV1>")]
pub enum ToolSetHistory {
    /// A checkpoint stored before the member existed, or folded from a WAL tail. Admission
    /// treats the checkpoint as incomplete and folds the complete WAL.
    #[default]
    Unknown,
    Known(ToolSetHistoryV1),
}

impl From<Option<ToolSetHistoryV1>> for ToolSetHistory {
    fn from(history: Option<ToolSetHistoryV1>) -> Self {
        history.map_or(Self::Unknown, Self::Known)
    }
}

impl From<ToolSetHistory> for Option<ToolSetHistoryV1> {
    fn from(history: ToolSetHistory) -> Self {
        match history {
            ToolSetHistory::Unknown => None,
            ToolSetHistory::Known(history) => Some(history),
        }
    }
}

impl ToolSetHistory {
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Fold the lineage's next run's [`tool_set_digest`].
    pub fn observe(&mut self, digest: String) {
        if let Self::Known(history) = self {
            history.changed |= history.last.as_ref().is_some_and(|last| *last != digest);
            history.last = Some(digest);
        }
    }

    /// The floor fold a run continues from when this checkpoint, rather than a replay of the
    /// whole WAL, describes the earlier runs. `None` when the checkpoint can't say: some run
    /// changed the tool set, so the floor's ordinal needs the whole WAL, or the history is
    /// unknown.
    pub fn continued_floor(&self) -> Option<SignedFloor> {
        match self {
            Self::Known(history) if !history.changed => {
                Some(SignedFloor::unchanged(history.last.clone()))
            }
            _ => None,
        }
    }
}

/// The floor a run admitted on a route that binds reasoning to its prefix starts from: `prior`'s
/// when the state it was admitted from folded every run (a replay of the whole WAL), else the
/// checkpoint's when it proves no run changed the tool set, else the floor a replay of the whole
/// WAL finds (`replay_whole_wal`).
pub fn admission_floor<E>(
    prior: FloorFold,
    checkpoint: &ToolSetHistory,
    replay_whole_wal: impl FnOnce() -> Result<FloorFold, E>,
) -> Result<FloorFold, E> {
    if prior.known().is_some() {
        return Ok(prior);
    }
    match checkpoint.continued_floor() {
        Some(floor) => Ok(FloorFold::Known(floor)),
        None => replay_whole_wal(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::request::ExecutionMode;
    use serde_json::json;

    fn tool(name: &str, description: &str) -> Tool {
        Tool {
            name: name.into(),
            description: description.into(),
            input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }
    }

    #[test]
    fn the_digest_counts_what_the_provider_sees_and_nothing_else() {
        let read = tool("read", "read a file");
        let grep = tool("grep", "search files");
        let base = tool_set_digest(&[read.clone(), grep.clone()], &Value::Null);
        // Order, duplicates and broca-side metadata don't count.
        assert_eq!(
            tool_set_digest(&[grep.clone(), read.clone(), read.clone()], &Value::Null),
            base
        );
        let mut dispatched = read.clone();
        dispatched.module = Some("fs".into());
        dispatched.execution_mode = ExecutionMode::Sequential;
        assert_eq!(
            tool_set_digest(&[dispatched, grep.clone()], &Value::Null),
            base
        );
        // A schema's key order doesn't count either: tools are compared canonically.
        let mut reordered = read.clone();
        reordered.input_schema = serde_json::from_str(
            r#"{"properties": {"path": {"type": "string"}}, "type": "object"}"#,
        )
        .unwrap();
        assert_eq!(
            tool_set_digest(&[reordered, grep.clone()], &Value::Null),
            base
        );
        // A changed description, schema or name, an added or a removed tool all count.
        let mut described = read.clone();
        described.description = "read a file, carefully".into();
        let mut schema = read.clone();
        schema.input_schema = json!({"type": "object"});
        let mut renamed = read.clone();
        renamed.name = "view".into();
        for changed in [
            vec![described, grep.clone()],
            vec![schema, grep.clone()],
            vec![renamed, grep.clone()],
            vec![read.clone()],
            vec![read.clone(), grep.clone(), tool("ls", "list files")],
        ] {
            assert_ne!(tool_set_digest(&changed, &Value::Null), base);
        }
        // An operator alias renames a tool on the wire; the submit tool is offered too.
        assert_ne!(
            tool_set_digest(
                &[read.clone(), grep.clone()],
                &json!({"tool_name_aliases": {"read": "Read"}})
            ),
            base
        );
        let submit = json!({"structured_output": {
            "mode": "submit_tool", "name": "result", "schema_jcs": "{\"type\":\"object\"}",
            "enforcement": "enforced", "delivery": "strict",
        }});
        assert!(FrozenStructuredOutput::from_frozen_render_config(Some(&submit)).is_some());
        assert_ne!(tool_set_digest(&[read, grep], &submit), base);
    }

    #[test]
    fn the_floor_moves_to_each_change_and_never_back() {
        let a = tool_set_digest(&[tool("read", "read a file")], &Value::Null);
        let b = tool_set_digest(&[tool("read", "read a file, carefully")], &Value::Null);
        let mut fold = SignedFloor::default();
        fold.observe_run(0, a.clone());
        fold.observe_run(4, a.clone());
        assert_eq!(
            fold.floor(),
            None,
            "an unchanged set keeps every run's reasoning"
        );
        fold.observe_run(9, b.clone());
        assert_eq!(fold.floor(), Some(9));
        fold.observe_run(15, b);
        assert_eq!(
            fold.floor(),
            Some(9),
            "a run with the same set keeps the floor"
        );
        // Back to the first set: the reasoning sent under B was signed under B, and A's was
        // already dropped, so the floor moves on rather than back.
        fold.observe_run(20, a);
        assert_eq!(fold.floor(), Some(20));
    }

    #[test]
    fn a_checkpoint_without_the_history_is_unknown_never_unchanged() {
        #[derive(Serialize, Deserialize)]
        struct Holder {
            #[serde(default, skip_serializing_if = "ToolSetHistory::is_unknown")]
            history: ToolSetHistory,
        }
        let absent: Holder = serde_json::from_str("{}").unwrap();
        assert_eq!(absent.history, ToolSetHistory::Unknown);
        assert_eq!(absent.history.continued_floor(), None);
        assert_eq!(serde_json::to_string(&absent).unwrap(), "{}");

        let mut history = ToolSetHistory::Known(ToolSetHistoryV1::default());
        history.observe("a".into());
        history.observe("a".into());
        assert_eq!(
            history.continued_floor(),
            Some(SignedFloor::unchanged(Some("a".into())))
        );
        let stored = serde_json::to_string(&Holder {
            history: history.clone(),
        })
        .unwrap();
        assert_eq!(stored, r#"{"history":{"last":"a"}}"#);
        let read: Holder = serde_json::from_str(&stored).unwrap();
        assert_eq!(read.history, history);

        history.observe("b".into());
        history.observe("a".into());
        assert_eq!(
            history.continued_floor(),
            None,
            "a lineage whose set ever changed needs the whole WAL for the floor's ordinal"
        );
    }
}
