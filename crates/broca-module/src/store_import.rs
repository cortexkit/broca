//! A head's import into the new storage: `session.import` with a file source, and
//! `session.install_view`.
//!
//! A head is a long-lived agent session another runner hosted. Its whole history becomes the
//! start of a new session here: one seeded run, written as a single synced batch, whose
//! messages keep their source ids, times, call keys and media (`ck_session_import::head` reads
//! them). The import is served by the session's store actor, under the session lease, so
//! nothing else writes beside it.
//!
//! The source is a file a caller names, which is a trust boundary: it is read only under a
//! configured import root, resolved after symlinks, and only when its SHA-256 matches the one
//! the caller gives. Once the session it fed is durable, the source is deleted, but only a file
//! inside broca's own import folder, and never a Pi journal.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use broca_protocol::content::Role;
use broca_protocol::{BindPrincipal, SessionScope};
use broca_session_store::record::{
    CallFacts, EntryFacts, ImportReceipt, ImportedMove, Input, Notice, Placement, Record,
    ResultAppend, RunEnd, RunStart, StepUsage, TurnFacts,
};
use broca_session_store::session::NewInputMedia;
use broca_wire::head_import::{
    FileImportParams, FileImportReply, ImportHarness, InstallViewReply, MoveList, MoveListShape,
    MoveRow,
};
use ck_session_import::head::{HeadError, HeadHistory, HeadMessage};
use sha2::{Digest as _, Sha256};

/// The folder under broca's state root that every configuration reads imports from by default,
/// and the only folder an import deletes its source from. The state root resolves the way every
/// CortexKit store's does (`XDG_DATA_HOME`, else the platform's data folder), so on a default
/// Linux or macOS install this is `~/.local/share/cortexkit/broca/import-sources/`. Thalamus's
/// `session.export` writes there.
pub const IMPORT_SOURCES_DIR: &str = "import-sources";

/// The import folder under broca's state root.
pub fn default_import_folder(state_root: &Path) -> PathBuf {
    state_root.join(IMPORT_SOURCES_DIR)
}

/// Where imports may read from, and the one folder whose files an import may delete.
#[derive(Debug, Clone)]
pub struct ImportRoots {
    roots: Vec<PathBuf>,
    /// Broca's own import folder: the only place a source file is ever deleted from.
    delete_folder: Option<PathBuf>,
}

impl ImportRoots {
    /// `roots` are the configured `import.roots`; `delete_folder` is broca's own import folder.
    pub fn new(roots: Vec<PathBuf>, delete_folder: Option<PathBuf>) -> Self {
        Self {
            roots,
            delete_folder,
        }
    }

    /// The configured roots, broca's own import folder under `home` by default.
    /// The configured `roots`, or broca's own import folder (`folder`) alone when none are
    /// configured. `folder` is the only place a source is deleted from either way.
    pub fn with_default(roots: Option<Vec<PathBuf>>, folder: Option<&Path>) -> Self {
        let folder = folder.map(Path::to_path_buf);
        let roots = roots.unwrap_or_else(|| folder.iter().cloned().collect());
        Self::new(roots, folder)
    }

    /// The file `path` names, resolved after symlinks, when it is a regular file inside one of
    /// the roots (each also resolved after symlinks). Anything else is refused
    /// `import_path_refused`, without saying which roots exist.
    pub fn admit(&self, path: &Path) -> Result<PathBuf, ImportRefusal> {
        let refused = || {
            ImportRefusal::new(
                "import_path_refused",
                format!(
                    "{} is not a file under a configured import root",
                    path.display()
                ),
            )
        };
        if !path.is_absolute() {
            return Err(refused());
        }
        let resolved = std::fs::canonicalize(path).map_err(|_| refused())?;
        let inside = self.roots.iter().any(|root| {
            std::fs::canonicalize(root)
                .is_ok_and(|root| resolved != root && resolved.starts_with(&root))
        });
        let file = std::fs::metadata(&resolved).is_ok_and(|meta| meta.is_file());
        if inside && file {
            Ok(resolved)
        } else {
            Err(refused())
        }
    }

    /// Whether `path` may be deleted once the session it fed is durable: it resolves, after
    /// symlinks, to a file inside broca's own import folder. A file anywhere else, or a link
    /// whose target lies outside the folder, is never deleted.
    pub fn deletable(&self, path: &Path) -> bool {
        let Some(folder) = self
            .delete_folder
            .as_ref()
            .and_then(|folder| std::fs::canonicalize(folder).ok())
        else {
            return false;
        };
        std::fs::canonicalize(path).is_ok_and(|resolved| {
            resolved != folder
                && resolved.starts_with(&folder)
                && std::fs::metadata(&resolved).is_ok_and(|meta| meta.is_file())
        })
    }
}

/// Deletes what fed an import once its session is durable: an OpenCode 2 export and its side
/// file, or for Pi only the side file, never the journal, wherever it lies. Each only when
/// [`ImportRoots::deletable`] allows it. A failed delete is logged: the session is durable, so
/// the import stands.
pub fn delete_sources(roots: &ImportRoots, harness: ImportHarness, source: &Path) {
    let sends = ck_session_import::head::sends_path(source);
    let mut doomed = vec![sends];
    if harness != ImportHarness::Pi {
        doomed.push(source.to_path_buf());
    }
    for path in doomed {
        if !roots.deletable(&path) {
            continue;
        }
        if let Err(error) = std::fs::remove_file(&path) {
            tracing::warn!(
                target: "broca.store_import",
                path = %path.display(),
                error = %error,
                "an import's source could not be deleted after its session became durable",
            );
        }
    }
}

/// An import or install refused, by code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRefusal {
    pub code: &'static str,
    pub message: String,
}

impl ImportRefusal {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<HeadError> for ImportRefusal {
    fn from(error: HeadError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}

/// A command only the store actor serves.
#[derive(Clone)]
pub enum StoreCommand {
    Import(Box<ImportCommand>),
    InstallView(Box<InstallViewCommand>),
    ApplyView(Box<ApplyViewCommand>),
}

/// `session.import` with a file source, as the serve layer hands it over.
#[derive(Clone)]
pub struct ImportCommand {
    pub params: FileImportParams,
    /// The attested principal of the route the import arrived on.
    pub principal: BindPrincipal,
    /// The route's scope, which the session freezes.
    pub scope: Option<SessionScope>,
    pub roots: Arc<ImportRoots>,
    pub now_ms: u64,
}

/// `session.install_view`.
#[derive(Clone)]
pub struct InstallViewCommand {
    pub view: Box<serde_json::value::RawValue>,
    pub principal: BindPrincipal,
}

/// `session.apply_view` on the `compaction-runner/v1` route, once the route admitted its caller
/// as the session's compaction provider and read the view.
#[derive(Clone)]
pub struct ApplyViewCommand {
    pub view: Box<serde_json::value::RawValue>,
}

/// How the store actor answered a [`StoreCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOutcome {
    Imported(FileImportReply),
    ViewInstalled(InstallViewReply),
    /// `session.apply_view`'s answer, as the route returns it: the view accepted, or refused
    /// with the provider rules' refusal.
    ViewApplied(String),
    Refused(ImportRefusal),
    /// The session closed under the command: route it again.
    Reopen,
}

/// The digest an import's retry is compared by: its key and every member of its payload.
pub fn payload_digest(params: &FileImportParams) -> String {
    let body = serde_json::to_vec(params).expect("import params serialize");
    format!("{:x}", Sha256::digest(body))
}

/// The run id of an import's seeded run, from its key.
pub fn seed_run_id(import_id: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(import_id.as_bytes()));
    format!("import-{}", &digest[..32])
}

/// What an import reads before it writes: the history, its media and the side file's facts,
/// from the file `params` names, once the path guard and its digest admit it.
pub fn read_source(
    params: &FileImportParams,
    roots: &ImportRoots,
) -> Result<(PathBuf, HeadHistory), ImportRefusal> {
    let path = roots.admit(Path::new(&params.source.path))?;
    let bytes = std::fs::read(&path).map_err(|error| {
        ImportRefusal::new(
            "import_source_unreadable",
            format!("the source file could not be read: {error}"),
        )
    })?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    if !digest.eq_ignore_ascii_case(&params.source.sha256) {
        return Err(ImportRefusal::new(
            "import_digest_mismatch",
            "the source file's SHA-256 is not the one the import names",
        ));
    }
    let mut check = |bytes: &[u8], mime: &str| {
        broca_core::media_validation::validate_media(bytes, mime)
            .map_err(|reason| format!("{reason:?}"))
    };
    let mut history = match params.source.harness {
        ImportHarness::OpenCode2 => ck_session_import::head::opencode2::read(&bytes, &mut check)?,
        ImportHarness::Pi => {
            ck_session_import::head::pi::read(&bytes, &params.source.session, &mut check)?
        }
        ImportHarness::OpenCode1 => {
            return Err(ImportRefusal::new(
                "import_harness_unsupported",
                "an OpenCode 1 source is not served yet",
            ))
        }
    };
    let sends = ck_session_import::head::sends_path(&path);
    if sends.exists() {
        let sends = roots.admit(&sends)?;
        let bytes = std::fs::read(&sends).map_err(|error| {
            ImportRefusal::new(
                "import_sends_unreadable",
                format!("the side file could not be read: {error}"),
            )
        })?;
        let rows = ck_session_import::head::read_sends(&bytes)?;
        ck_session_import::head::apply_sends(&mut history, rows)?;
    }
    Ok((path, history))
}

/// The highest part number the protocol's integers carry.
const MAX_PART_NUMBER: u64 = 9_007_199_254_740_991;

/// What an import writes: its batch, the media its messages carry, its id map and its receipt.
pub struct Seed {
    pub records: Vec<Record>,
    pub media: Vec<NewInputMedia>,
    pub ids: Vec<(String, u64)>,
    pub receipt: ImportReceipt,
}

/// What a seed records besides the history.
pub struct SeedFacts<'a> {
    pub params: &'a FileImportParams,
    pub principal: &'a BindPrincipal,
    pub scope: Option<&'a SessionScope>,
    pub now_ms: u64,
    /// The writer's message id prefix (`Session::message_id_prefix`) and the seq the batch's
    /// first record takes, which name a message the import makes itself.
    pub prefix: &'a str,
    pub first_seq: u64,
}

/// The import's seeded run: a run start, every imported message as an `Input` placed
/// `Imported` with its entry facts (its call facts and rides beside it), and the run's end.
///
/// The session is on the current compaction contract, as every new session is, so each message
/// records what `session.entries` serves: its part numbers, counted on from `tag_floor`; its
/// time, an earlier message's when the source has none; the step usage the source runner
/// served; and a user message's notice by the source runner's rule.
pub fn seed(history: HeadHistory, at: &SeedFacts<'_>) -> Result<Seed, ImportRefusal> {
    if history.messages.is_empty() {
        return Err(ImportRefusal::new(
            "import_empty",
            "the source holds no message to import",
        ));
    }
    if let Some(mid) = history
        .messages
        .iter()
        .filter_map(|message| message.mid.as_deref())
        .find(|mid| broca_session_store::session::is_broca_id_form(mid))
    {
        return Err(ImportRefusal::new(
            "import_id_refused",
            format!("source message id {mid} has the form of an id broca mints"),
        ));
    }
    let floor = at.params.tag_floor.unwrap_or(0);
    if floor > MAX_PART_NUMBER {
        return Err(ImportRefusal::new(
            "import_tag_floor_refused",
            format!("tag_floor {floor} is beyond the protocol's integer range"),
        ));
    }
    let run_id = seed_run_id(&at.params.import_id);
    let digest = payload_digest(at.params);
    let owner = broca_core::compaction_v2::entries::delivery_sender(
        Some(at.principal),
        broca_protocol::DeliveryMode::Steer,
    );
    let mut records = vec![Record::RunStart(Box::new(seed_start(
        &run_id, &digest, floor, at,
    )?))];
    let mut media = Vec::new();
    let mut ids = Vec::new();
    let mut high = floor;
    let mut time: Option<u64> = None;
    // The messages broca makes itself, by index, with the id each is stored under.
    let mut added: Vec<(usize, String)> = Vec::new();
    for (index, message) in history.messages.iter().enumerate() {
        let ordinal = index as u64 + 1;
        let notice = notice_of(message, owner.as_deref());
        records.push(Record::Input(Input {
            run_id: run_id.clone(),
            placement: Placement::Imported,
            message: message.message.clone(),
            written_at_ms: message.written_at_ms,
            submission_id: None,
            notice: notice.clone(),
            tags_v1: None,
            receipt_seq: None,
            pruned: Vec::new(),
            media: Vec::new(),
        }));
        let input_at = records.len() - 1;
        media.extend(message.media.iter().map(|medium| NewInputMedia {
            record: input_at,
            block: medium.block,
            part: medium.part,
            bytes: medium.bytes.clone(),
        }));
        if !message.calls.is_empty() {
            records.push(Record::TurnFacts(TurnFacts {
                ordinal,
                calls: message
                    .calls
                    .iter()
                    .map(|call| CallFacts {
                        tool_call_id: broca_protocol::ToolCallId(call.tool_call_id.clone()),
                        call_key: call.call_key.clone(),
                        schema_pin: None,
                        carrier: Some(call.carrier.clone()),
                        dispatched_to: None,
                        dispatched_at_ms: None,
                        finished_at_ms: None,
                        unanswered: None,
                    })
                    .collect(),
            }));
        }
        for ride in &message.rides {
            records.push(Record::ResultAppend(ResultAppend {
                run_id: run_id.clone(),
                tool_call_id: broca_protocol::ToolCallId(
                    ride.delivery_tool_call_id.clone().unwrap_or_default(),
                ),
                text: ride.text.clone().unwrap_or_default(),
                notice: Some(Notice {
                    sender: ride.sender.clone(),
                    template: ride.template.clone(),
                }),
                written_at_ms: None,
                send_id: Some(ride.send_id.clone()),
            }));
        }
        let mid = match &message.mid {
            Some(mid) => {
                ids.push((mid.clone(), ordinal));
                mid.clone()
            }
            // The facts record is the next one; a message broca makes is named by its seq.
            None => {
                let mid = format!("{}.{}", at.prefix, at.first_seq + records.len() as u64);
                added.push((index, mid.clone()));
                mid
            }
        };
        let written = message.written_at_ms.or(time).unwrap_or(0);
        time = Some(written);
        let send = message
            .send
            .as_ref()
            .filter(|_| message.message.role == Role::User);
        let ride = message.rides.last();
        records.push(Record::EntryFacts(EntryFacts {
            ordinal,
            written_at_ms: Some(written),
            tag_base: high,
            notice,
            usage: message.usage.map(|usage| StepUsage {
                prompt_tokens: usage.prompt_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                output_tokens: usage.output_tokens,
                reasoning_tokens: usage.reasoning_tokens,
            }),
            generated_under: None,
            // Imported messages came in as input: broca didn't write them.
            native: false,
            render_tags: false,
            clean_reply: false,
            mid,
            // An imported seed belongs to no run.
            run_id: None,
            created_at_ms: message.created_at_ms,
            delivery_mode: send
                .and_then(|send| send.delivery_mode.clone())
                .or_else(|| ride.and_then(|ride| ride.delivery_mode.clone())),
            delivery_tool_call_id: ride.and_then(|ride| ride.delivery_tool_call_id.clone()),
            client_message_id: send.and_then(|send| send.client_message_id.clone()),
            sender: send.map(|send| send.sender.clone()),
            marks: send.and_then(|send| send.marks.clone()),
            no_display_facts: None,
            reasoning_summaries: broca_session_loop::entries::reasoning_summaries(&message.message),
            rich_ui: None,
        }));
        high = high
            .checked_add(broca_core::compaction_v2::metadata::numbered_parts(
                &message.message,
            ))
            .filter(|high| *high <= MAX_PART_NUMBER)
            .ok_or_else(|| {
                ImportRefusal::new(
                    "import_tag_floor_refused",
                    "the imported parts' numbers above tag_floor exceed the protocol's integer \
                     range",
                )
            })?;
    }
    records.push(Record::RunEnd(RunEnd {
        run_id,
        reason: broca_protocol::RunFinishReason::Completed,
        usage: None,
        ts_ms: Some(at.now_ms),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    }));
    let receipt = ImportReceipt {
        import_id: at.params.import_id.clone(),
        digest,
        shape: shape_name(shape_of(at.params.source.harness)).into(),
        moves: moves_with_added(&history, added),
        imported_messages: history.messages.len() as u64,
        dropped_system_messages: history.dropped_system_messages as u64,
    };
    Ok(Seed {
        records,
        media,
        ids,
        receipt,
    })
}

/// The move list: one row per source row, in source order, and after each source row the
/// messages broca added that belong to it, in ordinal order: the tool results split off an
/// assistant row, or the results closing a call it left unanswered. An added message belongs to
/// the nearest source message before it. So every ordinal the import placed appears exactly once.
fn moves_with_added(history: &HeadHistory, added: Vec<(usize, String)>) -> Vec<ImportedMove> {
    // Each added message's owner: the index of the nearest earlier message a source row names.
    let mut after: std::collections::BTreeMap<Option<usize>, Vec<(usize, String)>> =
        Default::default();
    for (index, mid) in added {
        let owner = (0..index)
            .rev()
            .find(|earlier| history.messages[*earlier].mid.is_some());
        after.entry(owner).or_default().push((index, mid));
    }
    let row = |(index, mid): (usize, String)| ImportedMove {
        mid,
        ordinal: Some(index as u64 + 1),
    };
    let mut moves: Vec<ImportedMove> = after
        .remove(&None)
        .unwrap_or_default()
        .into_iter()
        .map(row)
        .collect();
    for source in &history.rows {
        moves.push(ImportedMove {
            mid: source.mid.clone(),
            ordinal: source.message.map(|index| index as u64 + 1),
        });
        if let Some(owned) = source.message.and_then(|index| after.remove(&Some(index))) {
            moves.extend(owned.into_iter().map(row));
        }
    }
    // An owner no source row names can't be told apart from its source rows: its added
    // messages close the list, in ordinal order, so none is left out.
    moves.extend(after.into_values().flatten().map(row));
    moves
}

/// A user message's notice, by the source runner's rule (Thalamus's `runner/compaction.rs`,
/// `notice_origins`): the sender of every send but the session owner's own plain prompts, with
/// the template it was rendered from. A message no send record names has none.
fn notice_of(message: &HeadMessage, owner: Option<&str>) -> Option<Notice> {
    if message.message.role != Role::User {
        return None;
    }
    let send = message.send.as_ref()?;
    if send.template.is_none() && owner == Some(send.sender.as_str()) {
        return None;
    }
    Some(Notice {
        sender: send.sender.clone(),
        template: send.template.clone(),
    })
}

/// The seeded run's start. It records the import's key and payload digest, so a retry is
/// recognised; the route's principal and scope, which make the importer the session's owner;
/// where the history came from; and the run settings today's import records on its seed
/// (`broca_core::import`): the placeholder model `import/import`, the digest that marks the
/// run as an import's, no tags rendered, and part numbers counting on from `floor`.
fn seed_start(
    run_id: &str,
    digest: &str,
    floor: u64,
    at: &SeedFacts<'_>,
) -> Result<RunStart, ImportRefusal> {
    let json = |value: serde_json::Result<serde_json::Value>| {
        value.map_err(|error| ImportRefusal::new("import_source_unmapped", error.to_string()))
    };
    let origin = json(serde_json::to_value(broca_wal::RunOrigin::SeededFrom {
        origin_kind: broca_wal::SeedOriginKind::Import,
        source_ref: at.params.origin.source_ref.clone(),
        origin_device_pseudonym: None,
        base_seq: None,
    }))?;
    let config = serde_json::json!({
        "account_limit_pause": false,
        "render_tags": false,
        "computed_tags": {"checkpoint": floor},
        "tools": [],
        "tool_choice": json(serde_json::to_value(broca_protocol::request::ToolChoice::None))?,
        "generation": json(serde_json::to_value(
            broca_protocol::request::GenerationConfig::default(),
        ))?,
        "response_format": json(serde_json::to_value(
            broca_protocol::request::ResponseFormat::default(),
        ))?,
        "provider_options": {"import_request_digest": digest},
        "cache_tiers": null,
        "context_epoch": 0,
        "frozen_render_config": null,
        "tools_were_omitted": false,
        "stop_when": [],
    });
    Ok(RunStart {
        run_id: run_id.to_owned(),
        model: broca_protocol::ModelHandle::new("import", "import"),
        setup: Vec::new(),
        renderer_version: 0,
        submission_id: None,
        send_id: Some(at.params.import_id.clone()),
        request_identity_digest: Some(digest.to_owned()),
        ts_ms: Some(at.now_ms),
        written_at_ms: Some(at.now_ms),
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
        auth_selection: None,
        run_choices: Some(serde_json::json!({ "config": config })),
        origin: Some(origin),
        principal: Some(json(serde_json::to_value(at.principal))?),
        scope: at
            .scope
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| ImportRefusal::new("import_source_unmapped", error.to_string()))?,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: Some(true),
        refusal_schedule: None,
        imported_ids: None,
    })
}

/// The move-list shape a source's head needs: a head on the standalone OpenCode 1 plugin gets
/// the plugin's, a head already on a runner the runner's.
pub fn shape_of(harness: ImportHarness) -> MoveListShape {
    match harness {
        ImportHarness::OpenCode1 => MoveListShape::Plugin,
        ImportHarness::OpenCode2 | ImportHarness::Pi => MoveListShape::Runner,
    }
}

fn shape_name(shape: MoveListShape) -> &'static str {
    match shape {
        MoveListShape::Plugin => "plugin",
        MoveListShape::Runner => "runner",
    }
}

/// The reply an import's receipt stands for, with the session's history version and
/// incarnation.
pub fn reply_of(
    receipt: &ImportReceipt,
    history_version: u64,
    incarnation: String,
) -> Result<FileImportReply, ImportRefusal> {
    let shape = match receipt.shape.as_str() {
        "plugin" => MoveListShape::Plugin,
        "runner" => MoveListShape::Runner,
        other => {
            return Err(ImportRefusal::new(
                "session_needs_recovery",
                format!("the import's receipt names an unknown move-list shape {other:?}"),
            ))
        }
    };
    Ok(FileImportReply {
        moves: MoveList {
            shape,
            rows: receipt
                .moves
                .iter()
                .map(|row| MoveRow {
                    mid: row.mid.clone(),
                    entry: row.ordinal,
                    finished_summary: None,
                    // No source id has the form of one broca mints (the import refuses one), so
                    // a row under such an id is a message broca added.
                    added: broca_session_store::session::is_broca_id_form(&row.mid),
                })
                .collect(),
        },
        history_version,
        incarnation,
        imported_messages: receipt.imported_messages,
        dropped_system_messages: receipt.dropped_system_messages,
    })
}

#[cfg(test)]
mod tests;
