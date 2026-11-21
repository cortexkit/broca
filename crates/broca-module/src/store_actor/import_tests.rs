//! The store actor's head import and view install, end to end over a session in memory: the
//! reply and what the session holds after it, retries, refusals, a cut at every write, and a
//! later run's request on the imported history.

use std::path::PathBuf;
use std::sync::Arc;

use broca_protocol::content::ContentBlock;
use broca_protocol::{BindPrincipal, SessionScope};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use broca_wire::head_import::{
    FileImportParams, FileImportReply, ImportHarness, ImportOrigin, ImportSource,
};
use sha2::Digest as _;
use tokio::sync::{mpsc, oneshot};

use super::tests::{executor, request, send_exactly, text_step};
use super::StoreActor;
use crate::actor::{LineageCmd, SendKey, SendOutcome};
use crate::store_import::{
    ImportCommand, ImportRoots, InstallViewCommand, StoreCommand, StoreOutcome,
};

#[path = "import_inline_tests.rs"]
mod inline;

// Imports staged and published on the session store's class roots.
#[path = "import_repetition_tests.rs"]
mod repetition;
#[path = "import_stage_tests.rs"]
mod staged;

const ADDR: &str = "fedcba9876543210";
const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==";

fn bind() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/project", "broca", "alfonso:head:a1")
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "alfonso:head:a1".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[9; 32])
}

fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn scope() -> SessionScope {
    SessionScope {
        owner: core(),
        scope_ref: "agent:a1".into(),
        scope_epoch: 1,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
        agent_id_frozen: false,
    }
}

/// An OpenCode 2 export: a prompt with an image, a system row, a reply that reasons, says
/// something and reads a file, and a last reply.
fn export() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look.",
             "files": [{"mime": "image/png", "data": PNG}]},
            {"id": "msg_sys", "type": "system", "time": {"created": 1100}, "text": "sys"},
            {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
             "tokens": {"input": 10, "output": 5, "reasoning": 1, "cache": {"read": 0, "write": 0}},
             "content": [
                {"type": "reasoning", "text": "Thinking it over.",
                 "native": {"signature": "sig-abc", "reasoningEncryptedContent": "enc",
                            "thoughtSignature": "tsig"}},
                {"type": "text", "text": "Reading the file."},
                {"type": "tool", "id": "call_1", "name": "read",
                 "state": {"status": "completed", "input": {"path": "a"},
                           "content": [{"type": "text", "text": "contents"}]}}
             ]},
            {"id": "msg_a2", "type": "assistant", "time": {"created": 4000, "completed": 4500},
             "content": [{"type": "text", "text": "Done."}]}
        ]
    }))
    .unwrap()
}

fn sends() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([
        {"mid": "msg_u1", "send_id": "s1", "client_message_id": "cm-1",
         "sender": "prefrontal-core", "delivered_as": "queue"},
        {"call_key": "msg_a1/call_1", "send_id": "s2", "sender": "occi",
         "delivered_as": "tool_result", "text": "\n\n[occi]"}
    ]))
    .unwrap()
}

/// A folder holding the export and its side file, as Thalamus writes them.
struct Source {
    dir: PathBuf,
    roots: Arc<ImportRoots>,
    /// The export's bytes.
    export: Vec<u8>,
    /// The side file's bytes, when there is one.
    sends: Option<Vec<u8>>,
}

impl Source {
    fn new(name: &str) -> Self {
        Self::of(name, export(), Some(sends()))
    }

    /// A source holding `export`, with the side file `sends` beside it when given.
    fn of(name: &str, export: Vec<u8>, sends: Option<Vec<u8>>) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "store-actor-import-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let roots = Arc::new(ImportRoots::new(vec![dir.clone()], Some(dir.clone())));
        let source = Self {
            dir,
            roots,
            export,
            sends,
        };
        source.write();
        source
    }

    fn path(&self) -> PathBuf {
        self.dir.join("head.json")
    }

    fn write(&self) {
        std::fs::write(self.path(), &self.export).unwrap();
        if let Some(sends) = &self.sends {
            std::fs::write(self.dir.join("head.sends.json"), sends).unwrap();
        }
    }

    fn params(&self, import_id: &str) -> FileImportParams {
        FileImportParams {
            import_id: import_id.into(),
            source: ImportSource {
                harness: ImportHarness::OpenCode2,
                path: self.path().display().to_string(),
                session: "ses_head".into(),
                sha256: format!("{:x}", sha2::Sha256::digest(&self.export)),
            },
            origin: ImportOrigin {
                kind: "import".into(),
                source_ref: "thalamus:ses_head".into(),
            },
            tag_floor: Some(40),
            retention: None,
            moves_from: None,
        }
    }

    fn import(&self, params: FileImportParams) -> StoreCommand {
        StoreCommand::Import(Box::new(ImportCommand {
            params,
            principal: core(),
            scope: Some(scope()),
            roots: Arc::clone(&self.roots),
            now_ms: 9_000,
        }))
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

type Served = tokio::task::JoinHandle<Session<&'static MemoryBackend>>;

fn serve(session: Session<&'static MemoryBackend>) -> (mpsc::UnboundedSender<LineageCmd>, Served) {
    let backend = *session.backend();
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor(Vec::new(), None)),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    )
    .with_import_staging(Arc::new(
        crate::store_import_stage::MemoryImportStaging::new(backend, deployment()),
    ));
    let (commands, receiver) = mpsc::unbounded_channel();
    (commands, tokio::spawn(actor.serve(receiver)))
}

async fn create(
    backend: &'static MemoryBackend,
) -> Result<Session<&'static MemoryBackend>, SessionError> {
    Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
}

async fn load(
    backend: &'static MemoryBackend,
) -> Result<Session<&'static MemoryBackend>, SessionError> {
    Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment()).await
}

async fn ask(commands: &mpsc::UnboundedSender<LineageCmd>, command: StoreCommand) -> StoreOutcome {
    let (reply, rx) = oneshot::channel();
    commands.send(LineageCmd::Store { command, reply }).unwrap();
    rx.await.unwrap()
}

fn imported(outcome: StoreOutcome) -> FileImportReply {
    match outcome {
        StoreOutcome::Imported(reply) => reply,
        other => panic!("not imported: {other:?}"),
    }
}

fn refused(outcome: StoreOutcome) -> &'static str {
    match outcome {
        StoreOutcome::Refused(refusal) => refusal.code,
        other => panic!("not refused: {other:?}"),
    }
}

/// An import asked for a page of its move list answers that page, on the import itself and on
/// every repeat, each repeat asking for any page; the rest of the reply is the unpaged reply's.
#[tokio::test]
async fn an_import_asked_for_a_page_answers_that_page_of_its_move_list() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("paged");
    let (commands, served) = serve(create(backend).await.unwrap());
    let page = |from: u64| {
        let mut params = source.params("imp-1");
        params.moves_from = Some(from);
        source.import(params)
    };
    let first = imported(ask(&commands, page(0)).await);
    let whole = imported(ask(&commands, source.import(source.params("imp-1"))).await);
    assert_eq!(whole.moves.rows.len(), 5);
    assert_eq!((whole.moves.total, whole.moves.next_from), (None, None));
    let mut unpaged = first.clone();
    unpaged.moves.total = None;
    assert_eq!(unpaged, whole, "the first page is the whole short list");
    assert_eq!((first.moves.total, first.moves.next_from), (Some(5), None));

    let rest = imported(ask(&commands, page(3)).await);
    assert_eq!(rest.moves.rows, whole.moves.rows[3..]);
    assert_eq!((rest.moves.total, rest.moves.next_from), (Some(5), None));
    assert_eq!(rest.newest, whole.newest);
    let end = imported(ask(&commands, page(5)).await);
    assert!(end.moves.rows.is_empty());
    assert_eq!((end.moves.total, end.moves.next_from), (Some(5), None));
    assert_eq!(refused(ask(&commands, page(6)).await), "invalid_params");
    drop(commands);
    served.await.unwrap();
}

/// An import writes the whole history as one seeded run and answers its move list; its source
/// files are deleted once it is durable; a retry is answered byte for byte from the receipt
/// after they're gone; another payload under its key, and another import, are refused; and the
/// imported session takes no send until it is adopted.
#[tokio::test]
async fn an_import_writes_the_history_and_answers_its_retry_from_the_receipt() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("whole");
    let (commands, served) = serve(create(backend).await.unwrap());
    let reply = imported(ask(&commands, source.import(source.params("imp-1"))).await);
    // The results split off `msg_a1` are broca's own message, listed right after it.
    every_ordinal_once(&reply);
    assert_eq!(
        moved(&reply.moves),
        [
            ("msg_u1".to_owned(), Some(1), false),
            ("msg_sys".to_owned(), None, false),
            ("msg_a1".to_owned(), Some(2), false),
            ("added".to_owned(), Some(3), true),
            ("msg_a2".to_owned(), Some(4), false)
        ]
    );
    assert_eq!(reply.imported_messages, 4);
    assert_eq!(reply.dropped_system_messages, 1);
    assert!(
        !source.path().exists(),
        "the export is deleted once durable"
    );
    assert!(!source.dir.join("head.sends.json").exists());

    let retry = imported(ask(&commands, source.import(source.params("imp-1"))).await);
    assert_eq!(
        serde_json::to_vec(&retry).unwrap(),
        serde_json::to_vec(&reply).unwrap()
    );
    let mut other_payload = source.params("imp-1");
    other_payload.tag_floor = Some(41);
    assert_eq!(
        refused(ask(&commands, source.import(other_payload)).await),
        "send_id_reuse"
    );
    assert_eq!(
        refused(ask(&commands, source.import(source.params("imp-2"))).await),
        "session_exists"
    );
    let outcome = send_exactly(
        &commands,
        "r1",
        {
            let mut next = request("next");
            next.principal = Some(core());
            next.scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
                scope: scope(),
                owner_authorized: true,
                kind: broca_protocol::ScopeKind::Head,
                parent: None,
                parent_state: None,
            });
            next
        },
        SendKey::Minted,
        broca_protocol::Delivery::Queue,
        None,
    )
    .await;
    assert!(
        matches!(
            &outcome,
            SendOutcome::DeliveryRefused {
                code: "invalid_params",
                field: "plan",
                ..
            }
        ),
        "{outcome:?}"
    );
    drop(commands);
    served.await.unwrap();

    let session = load(backend).await.unwrap();
    assert_eq!(session.conversation.placed, 4);
    // The seeded run reads back as today's import seed: a run config whose start marks it as
    // an import's, which today's rules treat as having no system prompt of its own.
    let seed = session.operations.owner.first_start.clone().unwrap();
    let config =
        broca_session_loop::run::run_config_of_start(&seed, &MemoryParts::default()).unwrap();
    assert!(broca_core::fresh_start::is_import(&config));
    assert_eq!(config.computed_tags.unwrap().checkpoint, 40);
    assert_eq!(session.find_message("msg_a1").await.unwrap(), Some(2));
    let entries = session.entries(1, 4).await.unwrap();
    let first = entries[0].state.facts.as_ref().unwrap();
    assert_eq!(first.mid, "msg_u1");
    assert_eq!(first.client_message_id.as_deref(), Some("cm-1"));
    assert_eq!(first.send_id.as_deref(), Some("s1"));
    assert_eq!(first.tag_base, 40);
    assert_eq!(first.written_at_ms, Some(1000));
    let turn = entries[1].state.turn.as_ref().unwrap();
    assert_eq!(turn.calls[0].call_key, "msg_a1/call_1");
    assert_eq!(turn.calls[0].carrier.as_deref(), Some("thalamus-harness"));
    assert_eq!(entries[1].state.delivered[0].text, "\n\n[occi]");
    // Reads serve the side file's facts and the calls' source keys, as for broca's own sends.
    let user = crate::store_read::display_facts(&entries[0]);
    assert_eq!(user.client_message_id.as_deref(), Some("cm-1"));
    assert_eq!(user.sender.as_deref(), Some("prefrontal-core"));
    assert_eq!(user.send_id.as_deref(), Some("s1"));
    let reply_facts = crate::store_read::display_facts(&entries[1]);
    assert_eq!(reply_facts.calls[0].call_key, "msg_a1/call_1");
    assert_eq!(
        reply_facts.calls[0].call_carrier.as_deref(),
        Some("thalamus-harness")
    );
    assert_eq!(reply_facts.notices[0].send_id.as_deref(), Some("s2"));
    assert_eq!(
        reply_facts.notices[0].call_key.as_deref(),
        Some("msg_a1/call_1")
    );
    assert_eq!(reply_facts.notices[0].sender.as_deref(), Some("occi"));
    let crate::store_read::StorePage { page, mids, .. } =
        crate::store_read::read_page(&session, Some(0), Some(10))
            .await
            .unwrap();
    assert_eq!(page.messages.len(), 4);
    // A read serves each imported message under the id the source gave it, and the results
    // message split out of `msg_a1` under the id broca minted for it.
    assert_eq!(
        [mids[0].as_str(), mids[1].as_str(), mids[3].as_str()],
        ["msg_u1", "msg_a1", "msg_a2"]
    );
    assert!(
        !mids[2].is_empty() && !mids[2].starts_with("msg_"),
        "{mids:?}"
    );
    assert_eq!(page.messages[0].created_at, Some(1000));
    assert!(page.messages.iter().all(|message| message.run.is_none()));
    // The image reads back from the stored record.
    let records = session.open_log_records().await.unwrap();
    let input = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Input(input) if !input.media.is_empty() => Some(input),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        session.input_media(input, 1, None).await.unwrap(),
        base64_png()
    );
    // No imported reasoning carries its provider payload.
    assert!(records.iter().all(|(_, record)| match record {
        Record::Input(input) => input.message.content.iter().all(|block| !matches!(
            block,
            ContentBlock::Reasoning {
                signature: Some(_),
                ..
            } | ContentBlock::RedactedReasoning { .. }
        )),
        _ => true,
    }));
}

fn base64_png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .unwrap()
}

/// Refusals that write nothing: a route without a scope, another owner's scope, and a path
/// outside the import roots.
#[tokio::test]
async fn an_import_from_anyone_but_its_owner_or_outside_the_roots_writes_nothing() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("refusals");
    let (commands, served) = serve(create(backend).await.unwrap());
    let StoreCommand::Import(base) = source.import(source.params("imp-1")) else {
        unreachable!()
    };
    let mut unscoped = base.clone();
    unscoped.scope = None;
    assert_eq!(
        refused(ask(&commands, StoreCommand::Import(unscoped)).await),
        "scope_required"
    );
    let mut stranger = base.clone();
    stranger.principal = BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    };
    assert_eq!(
        refused(ask(&commands, StoreCommand::Import(stranger)).await),
        "owner_required"
    );
    let mut elsewhere = base.clone();
    elsewhere.roots = Arc::new(ImportRoots::new(vec![source.dir.join("nowhere")], None));
    assert_eq!(
        refused(ask(&commands, StoreCommand::Import(elsewhere)).await),
        "import_path_refused"
    );
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(session.conversation.placed, 0);
    assert!(session.operations.run.is_none());
    assert!(source.path().exists());
}

fn view(version: u64) -> Box<serde_json::value::RawValue> {
    let view = broca_core::compaction_v2::protocol::view::View::seal(version, Vec::new());
    serde_json::value::to_raw_value(&view).unwrap()
}

fn install(principal: BindPrincipal, version: u64) -> StoreCommand {
    StoreCommand::InstallView(Box::new(InstallViewCommand {
        view: view(version),
        principal,
    }))
}

/// The owner installs the provider's first view as the applied view, version 1; a retry is
/// answered the same; another principal, another version or a session that isn't an import are
/// refused; and once the session has run a step the install is refused.
#[tokio::test]
async fn install_view_records_the_first_view_before_any_step() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("view");
    let (commands, served) = serve(create(backend).await.unwrap());
    assert_eq!(
        refused(ask(&commands, install(core(), 1)).await),
        "install_view_refused",
        "a session that isn't an import"
    );
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    let stranger = BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    };
    assert_eq!(
        refused(ask(&commands, install(stranger, 1)).await),
        "owner_required"
    );
    assert_eq!(
        refused(ask(&commands, install(core(), 2)).await),
        "invalid_params"
    );
    let StoreOutcome::ViewInstalled(installed) = ask(&commands, install(core(), 1)).await else {
        panic!("installed")
    };
    assert_eq!(installed.version, 1);
    assert_eq!(
        ask(&commands, install(core(), 1)).await,
        StoreOutcome::ViewInstalled(installed.clone())
    );
    drop(commands);
    let mut session = served.await.unwrap();
    let applied = session.operations.view_applied.clone().unwrap();
    assert_eq!(applied.view.version, 1);
    assert_eq!(applied.view.checksum, installed.checksum);
    assert_eq!(
        session.operations.view_accepted.as_ref().unwrap().version,
        1
    );
    // A later run's start: the session has now run, so a view comes from its provider.
    let mut start = session
        .operations
        .owner
        .first_start
        .clone()
        .expect("the seed's start");
    start.run_id = "r2".into();
    start.seeded = None;
    start.imported_ids = None;
    start.send_id = None;
    let end = Record::RunEnd(broca_session_store::record::RunEnd {
        run_id: "r2".into(),
        reason: broca_protocol::RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    });
    session
        .append(&[Record::RunStart(Box::new(start)), end], true)
        .await
        .unwrap();
    let (commands, served) = serve(session);
    assert_eq!(
        refused(ask(&commands, install(core(), 1)).await),
        "install_view_refused"
    );
    drop(commands);
    served.await.unwrap();
}

/// A cut at every write of the import, then a reload: the session holds either nothing (no
/// head, or the empty session its creation left) or the whole import, never part of it. A
/// retry of the import on the empty session answers what an uninterrupted import answers.
#[tokio::test]
async fn a_cut_at_every_write_leaves_no_session_or_the_whole_import() {
    let reference = {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let source = Source::new("reference");
        let (commands, served) = serve(create(backend).await.unwrap());
        let reply = imported(ask(&commands, source.import(source.params("imp-1"))).await);
        drop(commands);
        served.await.unwrap();
        reply
    };
    let mut cut = 0;
    loop {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let source = Source::new("cut");
        backend.crash_after(Some(cut));
        let finished = match create(backend).await {
            Err(_) => false,
            Ok(session) => {
                let (commands, served) = serve(session);
                let outcome = ask(&commands, source.import(source.params("imp-1"))).await;
                drop(commands);
                let _ = served.await;
                matches!(outcome, StoreOutcome::Imported(_))
            }
        };
        backend.crash_after(None);
        match load(backend).await {
            Err(SessionError::Missing) => assert!(!finished, "cut {cut}"),
            Err(other) => panic!("cut {cut}: the session doesn't load: {other:?}"),
            Ok(session) => {
                let placed = session.conversation.placed;
                if placed == 0 {
                    assert!(session.operations.run.is_none(), "cut {cut}");
                    assert!(session.operations.imported_ids.is_none(), "cut {cut}");
                    // The retry writes the whole import into the empty session.
                    source.write();
                    let (commands, served) = serve(session);
                    let retry =
                        imported(ask(&commands, source.import(source.params("imp-1"))).await);
                    // An added row's id is the session's own, so only its place is compared.
                    assert_eq!(moved(&retry.moves), moved(&reference.moves), "cut {cut}");
                    assert_eq!(retry.moves.shape, reference.moves.shape);
                    assert_eq!(retry.imported_messages, reference.imported_messages);
                    assert_eq!(retry.history_version, reference.history_version);
                    drop(commands);
                    served.await.unwrap();
                } else {
                    assert_eq!(placed, 4, "cut {cut}: a part of the import");
                    assert!(session.operations.run.as_ref().unwrap().finished.is_some());
                    assert_eq!(session.find_message("msg_a2").await.unwrap(), Some(4));
                    assert!(session.import_receipt().await.unwrap().is_some());
                }
            }
        }
        if finished {
            break;
        }
        cut += 1;
        assert!(cut < 200, "the import never finished");
    }
}

/// A later run's request on an imported history never sends a reasoning payload, since none was
/// imported: rendered for Anthropic, OpenAI Responses and Gemini, the imported reply's text goes
/// out and its reasoning doesn't. Every message id stays unique across the import and the run.
#[tokio::test]
async fn a_later_run_renders_the_imported_history_without_reasoning_payloads() {
    use broca_provider::families::{
        anthropic_frozen_config, gemini_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, Gemini, OpenAiResponses,
    };
    use broca_provider::wire::WireFamily;
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("render");
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    drop(commands);
    let mut session = served.await.unwrap();

    let provider = broca_core::provider::StubProvider::new(vec![text_step("m-next", "ok")]);
    let mut req = broca_core::RunRequest::from_submission(
        request("next"),
        broca_protocol::RunId("r-next".into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        ..Default::default()
    };
    let admission = broca_session_loop::admit::admission(
        &mut req,
        &control,
        &provider,
        session.operations(),
        Some(10_000),
    )
    .unwrap();
    let tools = broca_core::stub_plane::StubToolPlane::new();
    let now = || 10_000;
    let seams = broca_session_loop::run::Seams {
        person_steers: None,
        provider: &provider,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let mut parts = MemoryParts::default();
    broca_session_loop::run::run(&mut session, &mut parts, admission, &seams)
        .await
        .unwrap();
    let calls = provider.calls();
    let opts = calls.first().expect("the run sent a request");
    let prompt = serde_json::to_string(&opts.prompt).unwrap();
    assert!(prompt.contains("Reading the file."), "{prompt}");
    for payload in ["sig-abc", "enc", "tsig"] {
        assert!(!prompt.contains(payload), "{payload} in {prompt}");
    }
    let families: [(&dyn WireFamily, _); 3] = [
        (
            &AnthropicMessages,
            anthropic_frozen_config("claude-sonnet-4-5"),
        ),
        (&OpenAiResponses, openai_responses_frozen_config("gpt-5")),
        (&Gemini, gemini_frozen_config("gemini-2.5-pro")),
    ];
    for (family, frozen) in families {
        let body = String::from_utf8(family.render(&frozen, opts)).unwrap();
        assert!(body.contains("Reading the file."), "{body}");
        // Reasoning blocks and items of each family, and the payloads the source held. A
        // request's own thinking or reasoning settings are not history and may appear.
        for absent in [
            "Thinking it over.",
            "sig-abc",
            "\"type\":\"thinking\"",
            "\"type\":\"redacted_thinking\"",
            "\"type\":\"reasoning\"",
            "\"encrypted_content\":\"",
            "thoughtSignature",
            "\"thought\":true",
        ] {
            assert!(!body.contains(absent), "{absent} in {body}");
        }
    }
    let placed = session.conversation.placed;
    let entries = session.entries(1, placed as usize).await.unwrap();
    let mut mids: Vec<_> = entries
        .iter()
        .map(|entry| entry.state.facts.as_ref().unwrap().mid.clone())
        .collect();
    assert!(placed > 4, "the run placed its messages after the import");
    let all = mids.len();
    mids.sort();
    mids.dedup();
    assert_eq!(mids.len(), all, "a message id repeats");
}

/// An import's seeded run is a run start whose inputs are the imported messages in order: the
/// image of the imported user message is a person's, at that message's place (ordinal 0), and a
/// later request renders it with its bytes.
#[tokio::test]
async fn an_imported_image_is_a_persons_at_its_place_and_renders_with_its_bytes() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("media");
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    drop(commands);
    served.await.unwrap().close().await.unwrap();

    let mut session = load(backend).await.unwrap();
    let mut index = crate::store_media::SessionIndex::new(&session, Some(backend));
    index.open(&mut session, &MemoryParts::default()).await;
    assert!(index.ready(), "the import needs no build");
    let captures = index
        .index()
        .expect("the import captured its image")
        .session()
        .facts()
        .captures;
    assert_eq!(captures.len(), 1, "{captures:?}");
    assert!(
        captures[0].person,
        "an imported user message's image is a person's"
    );
    assert_eq!(captures[0].ordinal, Some(0));
    assert_eq!(captures[0].tool_call, None);

    let mut config = broca_provider::families::frozen_config_for_family(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        "m",
    );
    config.media = Some(broca_provider::media_profile::select_media_profile(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        Some(broca_provider::media_profile::MediaQuirk::Anthropic),
        "m",
        broca_provider::spec::InputModalities::from_value(&serde_json::json!(["text", "image"])),
        Some(200_000),
    ));
    let opts: broca_protocol::request::CallOptions = serde_json::from_value(
        serde_json::json!({ "prompt": session.conversation.held_messages().unwrap() }),
    )
    .unwrap();
    let body = index
        .for_run()
        .render(&serde_json::to_value(config).unwrap(), &opts, false, None)
        .unwrap();
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains(PNG), "{body}");
}

/// Through the registry: an import creates its session in the session store, never on today's,
/// and a retry is answered from it. Like any head, it needs only the store runtime: the switch
/// that starts every new session there stays off, as in production.
#[tokio::test]
async fn the_registry_creates_an_imported_session_in_the_new_storage() {
    use broca_core::FileLeaseStore;
    const IDLE: std::time::Duration = std::time::Duration::from_millis(100);
    let source = Source::new("registry");
    let dir = source.dir.join("state");
    let executor: Arc<dyn crate::executor::RunExecutor> = Arc::new(executor(Vec::new(), None));
    let runtime =
        crate::store_open::StoreRuntime::open(&dir, deployment(), Arc::clone(&executor), IDLE)
            .await
            .unwrap();
    let runtime = Arc::new(runtime);
    let opener = |runtime: Arc<crate::store_open::StoreRuntime>| {
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                Arc::clone(&executor),
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(runtime),
        )
    };
    let key = broca_protocol::BindIdentity::new(dir.join("project"), "broca", "alfonso:head:a1");
    assert!(!runtime.holds(&key).unwrap());
    let registry = crate::registry::LineageRegistry::new(opener(Arc::clone(&runtime)));
    let reply = imported(
        registry
            .route_store_command(&key, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(reply.imported_messages, 4);
    assert!(runtime.holds(&key).unwrap());
    let retry = imported(
        registry
            .route_store_command(&key, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(retry, reply);
    let StoreOutcome::ViewInstalled(_) = registry
        .route_store_command(&key, install(core(), 1))
        .await
        .unwrap()
    else {
        panic!("the owner installs the view")
    };
}

/// An import makes its route's principal the session's first owner, so it meets the session's
/// scope reservation as a first send does. Into a session another module's scope reserved, core's
/// import is refused as today's owner check refuses a send, naming the reserving owner, before its
/// source is read; the session created to admit it is gone once its actor closes. A reservation
/// whose row no longer decodes refuses the import as transient. Into a session core's own scope
/// reserved, the import is admitted.
#[tokio::test]
async fn an_import_meets_the_scope_reservation_of_the_session_it_creates() {
    use broca_core::scope_admission::OwnerMismatch;
    use broca_core::{FileLeaseStore, ScopeRefusal};
    use broca_store::session_address::ScopeReservation;
    const IDLE: std::time::Duration = std::time::Duration::from_millis(100);
    let source = Source::new("reserved");
    let dir = source.dir.join("state");
    std::fs::create_dir_all(&dir).unwrap();
    let executor: Arc<dyn crate::executor::RunExecutor> = Arc::new(executor(Vec::new(), None));
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(&dir, deployment(), Arc::clone(&executor), IDLE)
            .await
            .unwrap(),
    );
    let registry = crate::registry::LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
        )
        .with_idle_ttl(IDLE)
        .with_store_runtime(Arc::clone(&runtime)),
    ));
    let inventory = super::tests::Inventory::open(&dir, &dir);
    registry.set_session_address_inventory(Arc::clone(&inventory.store));
    let another = BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    };
    let session = |name: &str| {
        broca_protocol::BindIdentity::new(dir.join("project"), "broca", format!("alfonso:{name}"))
    };
    let (theirs, damaged, ours) = (session("theirs"), session("damaged"), session("ours"));
    for (key, owner) in [
        (&theirs, another.clone()),
        (&damaged, core()),
        (&ours, core()),
    ] {
        inventory.reserve(
            key,
            Some(&ScopeReservation {
                owner,
                scope_ref: scope().scope_ref,
            }),
        );
    }
    inventory.damage(&damaged);

    match registry
        .route_store_command(&theirs, source.import(source.params("imp-1")))
        .await
        .unwrap()
    {
        StoreOutcome::ScopeRefused(ScopeRefusal::OwnerMismatch(mismatch)) => assert!(
            matches!(
                &*mismatch,
                OwnerMismatch::Reserved { owner, presented_principal }
                    if *owner == another && *presented_principal == Some(core())
            ),
            "{mismatch:?}"
        ),
        other => panic!("expected the reservation's refusal, got {other:?}"),
    }
    for _ in 0..1000 {
        if registry.slot_state(&theirs).is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        !runtime.holds(&theirs).unwrap(),
        "the session created to admit the refused import is gone"
    );

    match registry
        .route_store_command(&damaged, source.import(source.params("imp-1")))
        .await
        .unwrap()
    {
        StoreOutcome::Transient(reason) => assert!(
            reason.starts_with("cannot read the session's scope reservation:"),
            "{reason}"
        ),
        other => panic!("expected a transient refusal, got {other:?}"),
    }

    let reply = imported(
        registry
            .route_store_command(&ours, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(reply.imported_messages, 4);
}

/// An import only ever brings a long-lived agent head onto broca, so the session it creates is
/// placed durable without any label, in the root backups capture, even when no backup classes
/// are configured. It reopens from there after a restart, and nothing of it is in the other
/// roots.
#[tokio::test]
async fn an_imported_head_is_created_in_the_durable_root_and_reopens_from_it() {
    use broca_core::FileLeaseStore;
    use broca_wal::StorageClass;
    const IDLE: std::time::Duration = std::time::Duration::from_millis(100);
    let source = Source::new("durable");
    let dir = source.dir.join("state");
    // No backup classes: an import's placement doesn't consult them.
    let executor: Arc<dyn crate::executor::RunExecutor> = Arc::new(executor(Vec::new(), None));
    let module = |runtime: Arc<crate::store_open::StoreRuntime>| {
        crate::registry::LineageRegistry::new(Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                Arc::clone(&executor),
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(runtime),
        ))
    };
    let open = || async {
        Arc::new(
            crate::store_open::StoreRuntime::open(&dir, deployment(), Arc::clone(&executor), IDLE)
                .await
                .unwrap(),
        )
    };
    let key = broca_protocol::BindIdentity::new(dir.join("project"), "broca", "alfonso:head:d1");
    let address = broca_core::session_addr(&key);
    let runtime = open().await;
    let registry = module(Arc::clone(&runtime));
    let reply = imported(
        registry
            .route_store_command(&key, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(reply.imported_messages, 4);
    assert_eq!(runtime.class_of(&key).unwrap(), Some(StorageClass::Durable));
    let written = crate::store_read::transcript_len(
        &runtime
            .current_view(&key)
            .await
            .unwrap()
            .expect("the imported session reads"),
    );
    assert!(written > 0);
    for class in StorageClass::ALL {
        assert_eq!(
            broca_wal::session_storage_head(&dir, class, &address).is_file(),
            class == StorageClass::Durable,
            "the {class} root's head"
        );
    }
    assert!(registry.stop_and_seal().await);

    // After a restart the session opens from the durable root, holding what the import wrote,
    // and a retry of the import is answered from its receipt there.
    let restarted = open().await;
    let registry = module(Arc::clone(&restarted));
    assert_eq!(
        restarted.class_of(&key).unwrap(),
        Some(StorageClass::Durable)
    );
    let view = restarted
        .current_view(&key)
        .await
        .unwrap()
        .expect("the imported session reads");
    assert_eq!(crate::store_read::transcript_len(&view), written);
    let retry = imported(
        registry
            .route_store_command(&key, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(retry, reply);
    assert_eq!(
        broca_wal::session_storage_class(&dir, &address).unwrap(),
        Some(StorageClass::Durable)
    );
    assert!(registry.stop_and_seal().await);
}

/// An imported history of four exchanges, each reply in two text parts, with no side file:
/// `msg_u1` to `msg_a4` take ordinals 1 to 8.
fn exchanges() -> Vec<u8> {
    let mut messages = Vec::new();
    for n in 1..=4u64 {
        messages.push(serde_json::json!({
            "id": format!("msg_u{n}"), "type": "user",
            "time": {"created": n * 1000}, "text": format!("Question {n}."),
        }));
        messages.push(serde_json::json!({
            "id": format!("msg_a{n}"), "type": "assistant",
            "time": {"created": n * 1000 + 100, "completed": n * 1000 + 200},
            "content": [
                {"type": "text", "text": format!("Answer {n} first part.")},
                {"type": "text", "text": format!("Answer {n} second part.")},
            ],
        }));
    }
    serde_json::to_vec(&serde_json::json!({"info": {"id": "ses_head"}, "messages": messages}))
        .unwrap()
}

use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};

fn drop_part(mid: &str, part: &str) -> EditOp {
    EditOp::Drop {
        mid: mid.into(),
        part: Some(part.into()),
    }
}

/// A view of version `version` holding `edits`, each with the version that first held it.
fn view_of(version: u64, edits: Vec<(u64, EditOp)>) -> View {
    View::seal(
        version,
        edits
            .into_iter()
            .map(|(since, op)| Edit::new(since, op))
            .collect(),
    )
}

fn install_view(view: &View) -> StoreCommand {
    StoreCommand::InstallView(Box::new(InstallViewCommand {
        view: serde_json::value::to_raw_value(view).unwrap(),
        principal: core(),
    }))
}

/// The view the head's compaction provider already applies when the head moves: it drops the
/// second part of `msg_a1` and the first part of `msg_a3`, both by their source ids.
fn installed_view() -> View {
    view_of(
        1,
        vec![(1, drop_part("msg_a1", "1")), (1, drop_part("msg_a3", "0"))],
    )
}

/// A compaction provider that answers Setup ready, takes every notice, and answers a ride with
/// the view set for it, once, or with nothing. It records every call.
#[derive(Default)]
struct Compactor {
    next: std::sync::Mutex<Option<View>>,
    calls: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
}

impl Compactor {
    fn answer_next_ride(&self, view: View) {
        *self.next.lock().unwrap() = Some(view);
    }

    /// Whether a notice told the provider a view was refused for naming the message `mid`,
    /// which the session doesn't hold.
    fn refused(&self, mid: &str) -> bool {
        let refusal = format!("\"code\":\"unknown_message\",\"mid\":\"{mid}\"");
        self.calls.lock().unwrap().iter().any(|(method, params)| {
            method == "compaction.notice" && params.to_string().contains(&refusal)
        })
    }
}

#[async_trait::async_trait]
impl broca_core::CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&SessionScope>,
        method: &str,
        params: serde_json::Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        self.calls.lock().unwrap().push((method.into(), params));
        let answer = match method {
            "compaction.setup" => serde_json::json!({"answer": "ready"}),
            "compaction.notice" => serde_json::json!({"answer": "received"}),
            "compaction.ride" => match self.next.lock().unwrap().take() {
                None => serde_json::json!({"answer": "nothing"}),
                Some(view) => serde_json::json!({"answer": "view", "view": view}),
            },
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// A fetch plan's manifest naming a compaction provider, `compactor` with preset `head`.
fn compacted_manifest() -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            serde_json::json!({
                "compaction_item": {"provider": "compactor", "preset": "head", "params": {}},
                "composition": {},
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: "plan".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(Vec::new()),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }
}

/// A writer over a session that seals nothing: when a view's cut applies, the history before
/// it stays in the open log, so the session renders the cut from its whole history.
struct Unsealed<'a>(&'a mut Session<&'static MemoryBackend>);

impl Writer for Unsealed<'_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        Writer::append(&mut *self.0, records, barrier).await
    }

    async fn caught_up(&mut self) -> Result<(), SessionError> {
        Writer::caught_up(&mut *self.0).await
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        Writer::send_records(&mut *self.0, submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(
            &'s broca_session_store::fold::conversation::Conversation,
            &'s broca_session_store::fold::operations::Operations,
            u64,
        ) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        Writer::append_with(&mut *self.0, build, barrier).await
    }

    fn conversation(&self) -> &broca_session_store::fold::conversation::Conversation {
        Writer::conversation(&*self.0)
    }

    fn operations(&self) -> &broca_session_store::fold::operations::Operations {
        Writer::operations(&*self.0)
    }

    fn session_id(&self) -> String {
        Writer::session_id(&*self.0)
    }

    fn next_seq(&self) -> u64 {
        Writer::next_seq(&*self.0)
    }

    fn retired_tag_floor(&self) -> u64 {
        Writer::retired_tag_floor(&*self.0)
    }

    fn message_id_prefix(&self) -> String {
        Writer::message_id_prefix(&*self.0)
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        Writer::open_log_records(&*self.0).await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        Writer::enrollment(&*self.0).await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Writer::put_enrollment(&mut *self.0, entries).await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        Writer::run_start(&*self.0, run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        Writer::window_message_whole(&*self.0, ordinal).await
    }

    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        Writer::release(&mut *self.0, blocks).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        Writer::tool_intent(&*self.0, call_key).await
    }

    async fn seal_at_entry(
        &mut self,
        _: u64,
        _: &broca_session_store::session::Carry<'_>,
        _: u64,
    ) -> Result<(), SessionError> {
        Ok(())
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_ledger(&mut *self.0, carry, now_ms).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_by_mid(&*self.0, mid).await
    }
    async fn entry_at(
        &self,
        ordinal: u64,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_at(&*self.0, ordinal).await
    }
}

/// A model step answering `text` in two text parts, `Reply to <text>` and `More on <text>`.
fn reply_step(id: &str, text: &str) -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: broca_protocol::content::AssistantMessage {
                message_id: broca_protocol::MessageId(id.into()),
                content: vec![
                    ContentBlock::Text {
                        text: format!("Reply to {text}").into(),
                    },
                    ContentBlock::Text {
                        text: format!("More on {text}").into(),
                    },
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: broca_protocol::Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Default::default()
            },
            finish_reason: broca_protocol::content::FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// Runs one run of one step on the session behind `writer`, on the model `model`, under the
/// compacted plan and `compactor`, and returns the request its step sent. A run on another
/// model than the request before it rides the compaction provider, which then may answer with
/// a view.
async fn run_once<W: Writer>(
    writer: &mut W,
    parts: &mut MemoryParts,
    compactor: &Compactor,
    run: (&str, &str),
    text: &str,
) -> broca_protocol::request::CallOptions {
    run_once_with(writer, parts, compactor, run, text, false).await
}

/// The stub provider; when `.1` is set, it reports the attempt a transport would before the
/// step's events, so the session records the step's request digest.
struct Attempting<'a>(&'a broca_core::provider::StubProvider, bool);

#[async_trait::async_trait]
impl broca_core::Provider for Attempting<'_> {
    async fn step(
        &self,
        step: broca_protocol::StepId,
        options: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        let digest = broca_core::render::request_digest_hex(&self.0.render_request(options));
        let mut inner = self.0.step(step, options).await;
        if !self.1 {
            return inner;
        }
        let attempt = serde_json::from_value(serde_json::json!({
            "attempt": 0, "request_body_len": 10, "request_body_sha256": digest,
            "http_status": 200, "response_body_len": 0,
            "response_body_sha256": "0".repeat(64), "outcome": "complete",
            "ts_ms": 1, "sent_at_ms": 1,
        }))
        .unwrap();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let _ = tx
                .send(broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AttemptFinished { attempt },
                ))
                .await;
            while let Some(event) = inner.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

/// [`run_once`], with each step recording its attempt when `attempts` is set.
async fn run_once_with<W: Writer>(
    writer: &mut W,
    parts: &mut MemoryParts,
    compactor: &Compactor,
    (run_id, model): (&str, &str),
    text: &str,
    attempts: bool,
) -> broca_protocol::request::CallOptions {
    let provider =
        broca_core::provider::StubProvider::new(vec![reply_step(&format!("m-{run_id}"), text)]);
    let mut submission = request(text);
    submission.tools = None;
    submission.policy_allowed = Default::default();
    submission.model = broca_protocol::ModelHandle::new("stub", model);
    let mut req = broca_core::RunRequest::from_submission(
        submission,
        broca_protocol::RunId(run_id.into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        manifest: Some(Arc::new(compacted_manifest())),
        admitted_context_limit: Some(200_000),
        // A new session starts on the current compaction contract; one that has run keeps its own.
        new_log_compaction_plan_version: Some(cortexkit_compaction_view::PLAN_VERSION),
        ..Default::default()
    };
    let admission = broca_session_loop::admit::admission(
        &mut req,
        &control,
        &provider,
        writer.operations(),
        Some(10_000),
    )
    .unwrap();
    let tools = broca_core::stub_plane::StubToolPlane::new();
    let now = || 10_000;
    let attempting = Attempting(&provider, attempts);
    let seams = broca_session_loop::run::Seams {
        person_steers: None,
        provider: &attempting,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: Some(compactor),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    // Boxed: the run loop's future is large, and a test holding it on its stack beside other
    // large futures overflows a debug build's smaller Windows test-thread stack.
    Box::pin(broca_session_loop::run::run(
        writer, parts, admission, &seams,
    ))
    .await
    .unwrap();
    let calls = provider.calls();
    assert_eq!(calls.len(), 1, "one step, one request");
    calls.into_iter().next().unwrap()
}

/// A session that imported [`exchanges`] and installed [`installed_view`].
async fn imported_with_view(
    name: &str,
) -> (
    &'static MemoryBackend,
    Session<&'static MemoryBackend>,
    Source,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of(name, exchanges(), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    assert!(matches!(
        ask(&commands, install_view(&installed_view())).await,
        StoreOutcome::ViewInstalled(_)
    ));
    drop(commands);
    (backend, served.await.unwrap(), source)
}

/// What the request's model reads: its messages, and the bytes one wire family renders, with the
/// runs that no longer shape the request left out, as a session that sealed history keeps them.
fn as_read(opts: &broca_protocol::request::CallOptions) -> (serde_json::Value, String) {
    use broca_provider::wire::WireFamily;
    let first_kept = opts
        .prompt
        .iter()
        .filter_map(|message| message.ordinal)
        .min()
        .unwrap_or(0) as usize;
    let mut opts = opts.clone();
    opts.render_episodes = Arc::new(
        broca_provider::families::openai_responses_chatgpt::reduce_episodes(
            &opts.render_episodes,
            first_kept,
        ),
    );
    let bytes = broca_provider::families::AnthropicMessages.render(
        &broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5"),
        &opts,
    );
    (
        serde_json::to_value(&opts.prompt).unwrap(),
        String::from_utf8(bytes).unwrap(),
    )
}

/// Installing a cutting view on an imported head that holds an image saves the session's image
/// index with the seal, so a later opening loads it instead of building it again from every
/// record before its first request.
#[tokio::test]
async fn an_installed_cut_saves_the_image_index() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::new("installed-cut-media");
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    let cut = view_of(
        1,
        vec![(
            1,
            EditOp::Cut {
                through: "msg_a1".into(),
            },
        )],
    );
    assert!(matches!(
        ask(&commands, install_view(&cut)).await,
        StoreOutcome::ViewInstalled(_)
    ));
    drop(commands);
    let session = served.await.unwrap();
    assert!(Writer::conversation(&session).window_start() > 1, "sealed");
    drop(session);
    let mut session = load(backend).await.unwrap();
    let loaded = session.feed_media().await.unwrap();
    assert!(
        matches!(
            loaded.saved,
            broca_session_store::session::SavedIndex::Saved(_)
        ),
        "{:?}",
        loaded.saved
    );
}

/// A view that cuts, installed before the first step, seals the history before its cut when it is
/// installed, so the imported session is no larger to open, read or run than the history its
/// requests render. Its first request is the one a session that recorded the same view without
/// sealing renders (and seals at that step, as a view applied at a step's start does). A later
/// step with no new view doesn't seal again, and a reload finds the window where the seal left it.
#[tokio::test]
async fn an_installed_cut_seals_the_history_before_the_first_step() {
    let cut = view_of(
        1,
        vec![(
            1,
            EditOp::Cut {
                through: "msg_a2".into(),
            },
        )],
    );
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("installed-cut", exchanges(), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    assert!(matches!(
        ask(&commands, install_view(&cut)).await,
        StoreOutcome::ViewInstalled(_)
    ));
    drop(commands);
    let mut session = served.await.unwrap();
    // msg_u3, the first kept entry, has ordinal 5.
    assert_eq!(
        Writer::conversation(&session).window_start(),
        5,
        "the installed cut sealed the history before it"
    );

    // The same import with the same view recorded as applied and nothing sealed.
    let whole_backend: &'static MemoryBackend = Box::leak(Box::default());
    let whole_source = Source::of("installed-cut-whole", exchanges(), None);
    let (commands, served) = serve(create(whole_backend).await.unwrap());
    imported(ask(&commands, whole_source.import(whole_source.params("imp-1"))).await);
    drop(commands);
    let mut whole = served.await.unwrap();
    let received = broca_core::compaction_v2::received::ReceivedView::parse(
        &serde_json::value::to_raw_value(&cut).unwrap(),
    )
    .unwrap();
    whole
        .append(
            &[
                broca_session_loop::compaction::view_record(None, &received, None).unwrap(),
                Record::ViewApplied(broca_session_store::record::ViewApplied {
                    step_id: 0,
                    version: 1,
                    checksum: cut.checksum.to_string(),
                    head_kept: None,
                }),
            ],
            true,
        )
        .await
        .unwrap();
    assert_eq!(Writer::conversation(&whole).window_start(), 1);

    let mut parts = MemoryParts::default();
    let compactor = Compactor::default();
    let first = run_once(
        &mut session,
        &mut parts,
        &compactor,
        ("r1", "m"),
        "Question 5.",
    )
    .await;
    let (mut whole_parts, whole_compactor) = (MemoryParts::default(), Compactor::default());
    let expected = run_once(
        &mut whole,
        &mut whole_parts,
        &whole_compactor,
        ("r1", "m"),
        "Question 5.",
    )
    .await;
    assert_eq!(as_read(&first), as_read(&expected));
    let (_, body) = as_read(&first);
    assert!(!body.contains("Question 2."), "{body}");
    assert!(body.contains("Question 3."), "{body}");
    assert_eq!(Writer::conversation(&whole).window_start(), 5);
    assert_eq!(Writer::conversation(&session).window_start(), 5);

    run_once(
        &mut session,
        &mut parts,
        &compactor,
        ("r2", "m"),
        "Question 6.",
    )
    .await;
    assert_eq!(Writer::conversation(&session).window_start(), 5);
    drop(session);
    let session = load(backend).await.unwrap();
    assert_eq!(Writer::conversation(&session).window_start(), 5);
}

/// A step rebuilt as of its own time renders the request it sent: the request dump reads the views
/// and state recorded up to the step, not the session as it stands now. The install's seal before
/// the step, a later view that changes what the session renders, and a later seal that cut deeper
/// than the step's own cut, moving its messages out of the session's window, don't change it.
#[tokio::test]
async fn a_step_rebuilt_as_of_its_time_ignores_later_views() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("as-of", exchanges(), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    let cut_a1 = |version, mut edits: Vec<(u64, EditOp)>| {
        edits.push((
            1,
            EditOp::Cut {
                through: "msg_a1".into(),
            },
        ));
        view_of(version, edits)
    };
    assert!(matches!(
        ask(
            &commands,
            install_view(&cut_a1(1, vec![(1, drop_part("msg_a3", "0"))]))
        )
        .await,
        StoreOutcome::ViewInstalled(_)
    ));
    drop(commands);
    let mut session = served.await.unwrap();
    assert_eq!(
        Writer::conversation(&session).window_start(),
        3,
        "installed seal"
    );
    let cutter = Compactor::default();
    let mut parts = MemoryParts::default();
    let first = run_once_with(
        &mut session,
        &mut parts,
        &cutter,
        ("r1", "m"),
        "Question 5.",
        true,
    )
    .await;
    // The next view drops a part the first one kept, with the same cut.
    cutter.answer_next_ride(cut_a1(
        2,
        vec![(1, drop_part("msg_a3", "0")), (2, drop_part("msg_a4", "0"))],
    ));
    run_once(
        &mut session,
        &mut parts,
        &cutter,
        ("r2", "m2"),
        "Question 6.",
    )
    .await;
    drop(session);
    let session = load(backend).await.unwrap();
    let records = |session: &Session<&'static MemoryBackend>| {
        let stored = session.stored_records(backend);
        let last = session.next_seq() - 1;
        async move { stored.records_between(1, last).await.unwrap() }
    };
    let history = records(&session).await;
    let rebuilt = Box::pin(super::recorded_warm::request_as_of(
        &session,
        &history,
        &parts,
        &bind(),
        "r1",
        1,
    ))
    .await
    .unwrap();
    assert_eq!(as_read(&rebuilt.opts), as_read(&first));
    // Read from the session as it stands now, the same step takes the later view.
    let current = Box::pin(super::recorded_warm::request(
        &session,
        &history,
        &parts,
        &bind(),
        "r1",
        Some(1),
    ))
    .await
    .map(|request| as_read(&request.opts));
    assert_ne!(current.ok(), Some(as_read(&first)));

    // A view that cuts deeper seals the history through `msg_a3` at the next step.
    let mut session = session;
    cutter.answer_next_ride(view_of(
        3,
        vec![
            (1, drop_part("msg_a3", "0")),
            (2, drop_part("msg_a4", "0")),
            (
                3,
                EditOp::Cut {
                    through: "msg_a3".into(),
                },
            ),
        ],
    ));
    let third = run_once_with(
        &mut session,
        &mut parts,
        &cutter,
        ("r3", "m3"),
        "Question 7.",
        true,
    )
    .await;
    assert_eq!(
        Writer::conversation(&session).window_start(),
        7,
        "deeper seal"
    );
    let history = records(&session).await;
    let rebuilt = Box::pin(super::recorded_warm::request_as_of(
        &session,
        &history,
        &parts,
        &bind(),
        "r1",
        1,
    ))
    .await
    .unwrap();
    assert_eq!(as_read(&rebuilt.opts), as_read(&first));
    // A step after that seal ran in the log the seal opened, which starts with whole copies of the
    // views its stored views were changes against; its history holds both.
    let rebuilt = super::recorded_warm::request_as_of(&session, &history, &parts, &bind(), "r3", 1)
        .await
        .unwrap();
    assert_eq!(as_read(&rebuilt.opts), as_read(&third));
}

/// The compaction provider's view names imported messages by their source ids. Installed, it applies at
/// the first step; a later view cuts through the imported history, which seals it; and after a
/// reload, the next step's request is what the same views give on a session that sealed
/// nothing, every source id the views name still resolves, and a view naming an id the import
/// never had is refused by name, leaving the applied view in place.
#[tokio::test]
async fn an_imported_sessions_view_applies_after_a_seal_through_the_import() {
    let (backend, mut sealed, _source_a) = imported_with_view("sealed").await;
    let (_, mut whole, _source_b) = imported_with_view("whole").await;
    let (cutter, keeper) = (Compactor::default(), Compactor::default());
    let (mut sealed_parts, mut whole_parts) = (MemoryParts::default(), MemoryParts::default());

    // The installed view applies at the first step.
    let first = run_once(
        &mut sealed,
        &mut sealed_parts,
        &cutter,
        ("r1", "m"),
        "Question 5.",
    )
    .await;
    let unsealed = run_once(
        &mut Unsealed(&mut whole),
        &mut whole_parts,
        &keeper,
        ("r1", "m"),
        "Question 5.",
    )
    .await;
    let (_, body) = as_read(&first);
    assert!(body.contains("Answer 1 first part."), "{body}");
    assert!(!body.contains("Answer 1 second part."), "{body}");
    assert!(!body.contains("Answer 3 first part."), "{body}");
    assert!(body.contains("Answer 3 second part."), "{body}");
    assert_eq!(as_read(&first), as_read(&unsealed));

    // The provider's next view keeps both drops and cuts through `msg_a2`: the session that
    // seals moves the history up to it out of its open log.
    let cut = view_of(
        2,
        vec![
            (1, drop_part("msg_a1", "1")),
            (1, drop_part("msg_a3", "0")),
            (
                2,
                EditOp::Cut {
                    through: "msg_a2".into(),
                },
            ),
        ],
    );
    cutter.answer_next_ride(cut.clone());
    keeper.answer_next_ride(cut.clone());
    run_once(
        &mut sealed,
        &mut sealed_parts,
        &cutter,
        ("r2", "m2"),
        "Question 6.",
    )
    .await;
    run_once(
        &mut Unsealed(&mut whole),
        &mut whole_parts,
        &keeper,
        ("r2", "m2"),
        "Question 6.",
    )
    .await;
    let diagnosis = format!(
        "applied {:?}, accepted {:?}, calls {:?}",
        Writer::operations(&sealed)
            .view_applied
            .as_ref()
            .map(|applied| (applied.step_id, applied.view.version)),
        Writer::operations(&sealed)
            .view_accepted
            .as_ref()
            .map(|accepted| accepted.version),
        cutter
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        Writer::conversation(&sealed).window_start(),
        5,
        "sealed at msg_u3: {diagnosis}"
    );
    assert_eq!(Writer::conversation(&whole).window_start(), 1);

    drop(sealed);
    let mut sealed = load(backend).await.unwrap();
    assert_eq!(Writer::conversation(&sealed).window_start(), 5);
    for (mid, ordinal) in [("msg_a1", 2), ("msg_a2", 4), ("msg_a3", 6)] {
        assert_eq!(
            sealed.find_message(mid).await.unwrap(),
            Some(ordinal),
            "{mid}"
        );
        let entry = Writer::entry_by_mid(&sealed, mid).await.unwrap().unwrap();
        assert_eq!(entry.ordinal, ordinal);
        assert_eq!(entry.state.facts.as_ref().unwrap().mid, mid);
    }

    // The provider's next view also drops a message the import never had: it is refused, by
    // name, and the cut view still applies.
    let unknown = view_of(
        3,
        vec![
            (1, drop_part("msg_a1", "1")),
            (1, drop_part("msg_a3", "0")),
            (
                2,
                EditOp::Cut {
                    through: "msg_a2".into(),
                },
            ),
            (
                3,
                EditOp::Drop {
                    mid: "msg_never".into(),
                    part: None,
                },
            ),
        ],
    );
    cutter.answer_next_ride(unknown.clone());
    keeper.answer_next_ride(unknown);
    let after = run_once(
        &mut sealed,
        &mut sealed_parts,
        &cutter,
        ("r3", "m3"),
        "Question 7.",
    )
    .await;
    let expected = run_once(
        &mut Unsealed(&mut whole),
        &mut whole_parts,
        &keeper,
        ("r3", "m3"),
        "Question 7.",
    )
    .await;
    assert_eq!(as_read(&after), as_read(&expected));
    let (_, body) = as_read(&after);
    for gone in [
        "Question 1.",
        "Answer 2 second part.",
        "Answer 3 first part.",
    ] {
        assert!(!body.contains(gone), "{gone} in {body}");
    }
    for kept in ["Question 3.", "Answer 3 second part.", "Question 7."] {
        assert!(body.contains(kept), "{kept} not in {body}");
    }
    assert!(
        cutter.refused("msg_never"),
        "the refusal names the id the import never had"
    );
    assert!(
        !cutter.refused("msg_a1"),
        "a sealed imported message is known"
    );
    let operations = Writer::operations(&sealed);
    assert_eq!(operations.view_applied.as_ref().unwrap().view.version, 2);
    assert_eq!(operations.view_accepted.as_ref().unwrap().version, 2);
}

/// An installed view must fit the imported history: one naming a message the import never had,
/// or a part its message doesn't have, is refused naming it, and nothing is installed.
#[tokio::test]
async fn an_installed_view_naming_what_the_import_never_had_is_refused_by_name() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("unknown-view", exchanges(), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    for (op, named) in [
        (
            EditOp::Drop {
                mid: "msg_never".into(),
                part: None,
            },
            "msg_never",
        ),
        (drop_part("msg_a1", "9"), "\"9\""),
    ] {
        match ask(&commands, install_view(&view_of(1, vec![(1, op)]))).await {
            StoreOutcome::Refused(refusal) => {
                assert_eq!(refusal.code, "invalid_params");
                assert!(refusal.message.contains(named), "{}", refusal.message);
            }
            other => panic!("installed: {other:?}"),
        }
    }
    drop(commands);
    let session = served.await.unwrap();
    assert!(session.operations.view_applied.is_none());
    assert!(session.operations.view_accepted.is_none());
}

/// The message ids of the entries at `ordinals`, as the session holds them.
async fn mids_at(session: &Session<&'static MemoryBackend>, ordinals: &[u64]) -> Vec<String> {
    let mut mids = Vec::new();
    for ordinal in ordinals {
        let entry = session.entries(*ordinal, 1).await.unwrap().remove(0);
        mids.push(entry.state.facts.unwrap().mid.clone());
    }
    mids
}

/// The same as the imported case, on a session with no import, whose sealed messages carry the
/// ids broca minted: a view carrying a drop on a sealed message still checks, and one naming a
/// message the session never had is refused by that id, while the step after the seal sends
/// what the same view gives on a session that sealed nothing.
#[tokio::test]
async fn a_view_carrying_edits_on_sealed_messages_still_checks_without_an_import() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut sealed = create(backend).await.unwrap();
    let whole_backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut whole = create(whole_backend).await.unwrap();
    let (cutter, keeper) = (Compactor::default(), Compactor::default());
    let (mut sealed_parts, mut whole_parts) = (MemoryParts::default(), MemoryParts::default());
    for (run, text) in [
        ("r1", "Question 1."),
        ("r2", "Question 2."),
        ("r3", "Question 3."),
    ] {
        run_once(&mut sealed, &mut sealed_parts, &cutter, (run, "m"), text).await;
        run_once(
            &mut Unsealed(&mut whole),
            &mut whole_parts,
            &keeper,
            (run, "m"),
            text,
        )
        .await;
    }
    // Each session's own ids for its first, second and third replies: ordinals 2, 4 and 6.
    let views = |mids: &[String], with_unknown: bool| {
        let mut edits = vec![
            (1, drop_part(&mids[0], "1")),
            (1, drop_part(&mids[2], "1")),
            (
                1,
                EditOp::Cut {
                    through: mids[1].clone(),
                },
            ),
        ];
        if with_unknown {
            edits.push((
                2,
                EditOp::Drop {
                    mid: "msg_never".into(),
                    part: None,
                },
            ));
        }
        view_of(if with_unknown { 2 } else { 1 }, edits)
    };
    let sealed_mids = mids_at(&sealed, &[2, 4, 6]).await;
    let whole_mids = mids_at(&whole, &[2, 4, 6]).await;
    cutter.answer_next_ride(views(&sealed_mids, false));
    keeper.answer_next_ride(views(&whole_mids, false));
    run_once(
        &mut sealed,
        &mut sealed_parts,
        &cutter,
        ("r4", "m2"),
        "Question 4.",
    )
    .await;
    run_once(
        &mut Unsealed(&mut whole),
        &mut whole_parts,
        &keeper,
        ("r4", "m2"),
        "Question 4.",
    )
    .await;
    assert_eq!(Writer::conversation(&sealed).window_start(), 5);
    assert_eq!(Writer::conversation(&whole).window_start(), 1);

    drop(sealed);
    let mut sealed = load(backend).await.unwrap();
    for (mid, ordinal) in sealed_mids.iter().zip([2, 4, 6]) {
        assert_eq!(sealed.find_message(mid).await.unwrap(), Some(ordinal));
    }
    cutter.answer_next_ride(views(&sealed_mids, true));
    keeper.answer_next_ride(views(&whole_mids, true));
    let after = run_once(
        &mut sealed,
        &mut sealed_parts,
        &cutter,
        ("r5", "m3"),
        "Question 5.",
    )
    .await;
    let expected = run_once(
        &mut Unsealed(&mut whole),
        &mut whole_parts,
        &keeper,
        ("r5", "m3"),
        "Question 5.",
    )
    .await;
    assert_eq!(as_read(&after), as_read(&expected));
    let (_, body) = as_read(&after);
    assert!(!body.contains("Reply to Question 2."), "{body}");
    assert!(!body.contains("More on Question 3."), "{body}");
    assert!(body.contains("Reply to Question 3."), "{body}");
    assert!(cutter.refused("msg_never"));
    assert!(keeper.refused("msg_never"));
    assert!(
        !cutter.refused(&sealed_mids[0]),
        "a sealed message is known"
    );
    assert_eq!(
        Writer::operations(&sealed)
            .view_applied
            .as_ref()
            .unwrap()
            .view
            .version,
        1
    );
}

/// An imported history of `exchanges` exchanges, each reply in two text parts.
fn many_exchanges(exchanges: u64) -> Vec<u8> {
    let messages: Vec<_> = (1..=exchanges)
        .flat_map(|n| {
            [
                serde_json::json!({
                    "id": format!("msg_u{n}"), "type": "user",
                    "time": {"created": n * 10}, "text": format!("Question {n}."),
                }),
                serde_json::json!({
                    "id": format!("msg_a{n}"), "type": "assistant",
                    "time": {"created": n * 10 + 1, "completed": n * 10 + 2},
                    "content": [
                        {"type": "text", "text": format!("Answer {n} first part.")},
                        {"type": "text", "text": format!("Answer {n} second part.")},
                    ],
                }),
            ]
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({"info": {"id": "ses_head"}, "messages": messages}))
        .unwrap()
}

/// A view naming 800 sealed messages reads each of them once while the session is open: the
/// first `transcript()` after a reload reads them back from sealed history, and every later one
/// borrows what the first built. Both times are written to
/// `target/scratch/transcript-timing.txt`.
#[tokio::test]
async fn a_view_naming_800_sealed_messages_reads_them_once() {
    const NAMED: u64 = 800;
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("many", many_exchanges(NAMED + 1), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    drop(commands);
    let mut session = served.await.unwrap();
    let compactor = Compactor::default();
    let mut parts = MemoryParts::default();
    run_once(&mut session, &mut parts, &compactor, ("r1", "m"), "Next.").await;
    let mut edits: Vec<(u64, EditOp)> = (1..=NAMED)
        .map(|n| (1, drop_part(&format!("msg_a{n}"), "1")))
        .collect();
    edits.push((
        1,
        EditOp::Cut {
            through: format!("msg_a{NAMED}"),
        },
    ));
    compactor.answer_next_ride(view_of(1, edits));
    run_once(&mut session, &mut parts, &compactor, ("r2", "m2"), "After.").await;
    assert_eq!(
        Writer::conversation(&session).window_start(),
        2 * NAMED + 1,
        "sealed through the cut"
    );
    drop(session);
    let session = load(backend).await.unwrap();

    let state = broca_session_loop::compaction::restore(
        &Writer::open_log_records(&session).await.unwrap(),
        &bind(),
        Writer::operations(&session),
    )
    .unwrap();
    let compaction = Arc::new(broca_core::compaction_v2::session::Session::default());
    *compaction.state.lock().await = state;
    let ctx = broca_session_loop::compaction::Ctx {
        session: compaction,
        provider: "compactor".into(),
        preset: "head".into(),
        earlier: None,
        window: Default::default(),
    };
    let started = std::time::Instant::now();
    let first = broca_session_loop::compaction::transcript(&session, &ctx)
        .await
        .unwrap();
    let cold = started.elapsed();
    assert_eq!(
        first.sealed.len() as u64,
        NAMED,
        "every named sealed message"
    );
    let expected = (*first.entries).clone();
    let built = Arc::as_ptr(&first.entries);
    // A step's transcript is gone before the next step asks for one.
    drop(first);
    let mut warm = Vec::new();
    for _ in 0..5 {
        let started = std::time::Instant::now();
        let later = broca_session_loop::compaction::transcript(&session, &ctx)
            .await
            .unwrap();
        warm.push(started.elapsed());
        assert_eq!(*later.entries, expected);
        assert_eq!(
            Arc::as_ptr(&later.entries),
            built,
            "a later step borrows the entries built before, copying none"
        );
    }
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    std::fs::write(
        scratch.join("transcript-timing.txt"),
        format!(
            "named sealed messages: {NAMED}\nfirst transcript(): {cold:?}\nlater transcript(): {warm:?}\n"
        ),
    )
    .unwrap();
}

/// One of Thalamus's published exports (`ck-session-import`'s `thalamus-export-v1` fixtures),
/// imported through `session.import` into a new session: the reply, the session as a reload
/// finds it, and Thalamus's own export reply.
async fn import_thalamus(
    dir: &str,
    journal: &str,
    harness: ImportHarness,
    session: &str,
) -> (
    FileImportReply,
    Session<&'static MemoryBackend>,
    serde_json::Value,
) {
    let fixture = |name: &str| {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../ck-session-import/tests/fixtures/thalamus-export-v1")
                .join(dir)
                .join(name),
        )
        .unwrap()
    };
    let export: serde_json::Value = serde_json::from_slice(&fixture("reply.json")).unwrap();
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of(dir, fixture(journal), Some(fixture("export.sends.json")));
    let mut params = source.params("imp-thalamus");
    params.source.harness = harness;
    params.source.session = session.into();
    params.source.sha256 = export["sha256"].as_str().unwrap().into();
    params.tag_floor = export["tag_floor"].as_u64();
    let (commands, served) = serve(create(backend).await.unwrap());
    let reply = imported(ask(&commands, source.import(params)).await);
    drop(commands);
    served.await.unwrap();
    assert!(
        !source.dir.join("head.sends.json").exists(),
        "the side file is gone"
    );
    (reply, load(backend).await.unwrap(), export)
}

/// What every imported export must show: the newest imported message is the one Thalamus's
/// reply names, and it is the last row the move list places; the first message's parts are
/// numbered from the reply's `tag_floor`, and each later message's parts after the earlier
/// messages' parts.
async fn newest_and_floor(
    reply: &FileImportReply,
    session: &Session<&'static MemoryBackend>,
    export: &serde_json::Value,
) -> Vec<broca_session_store::session::StoredEntry> {
    let placed = session.conversation.placed;
    assert_eq!(placed, reply.imported_messages);
    let entries = session.entries(1, placed as usize).await.unwrap();
    let newest = export["newest_mid"].as_str().unwrap();
    assert_eq!(
        entries.last().unwrap().state.facts.as_ref().unwrap().mid,
        newest
    );
    let last_placed = reply
        .moves
        .rows
        .iter()
        .rev()
        .find(|row| row.entry.is_some())
        .unwrap();
    assert_eq!(
        (last_placed.mid.as_str(), last_placed.entry),
        (newest, Some(placed))
    );
    let bases: Vec<u64> = entries
        .iter()
        .map(|entry| entry.state.facts.as_ref().unwrap().tag_base)
        .collect();
    assert_eq!(bases[0], export["tag_floor"].as_u64().unwrap());
    assert!(bases.windows(2).all(|pair| pair[0] <= pair[1]), "{bases:?}");
    assert!(bases.last().unwrap() > &bases[0], "{bases:?}");
    entries
}

/// A move list's rows as `(mid, entry, added)`, with an added row's id, which names a seq of
/// the session that holds it, written `added`.
fn moved(moves: &broca_wire::head_import::MoveList) -> Vec<(String, Option<u64>, bool)> {
    moves
        .rows
        .iter()
        .map(|row| {
            let mid = if row.added { "added" } else { &row.mid };
            (mid.to_owned(), row.entry, row.added)
        })
        .collect()
}

/// The move list names every ordinal the import placed exactly once, source rows and the
/// messages broca added alike, in ordinal order; an added row's id is broca's, and every other
/// row is a source row.
fn every_ordinal_once(reply: &FileImportReply) {
    let entries: Vec<u64> = reply
        .moves
        .rows
        .iter()
        .filter_map(|row| row.entry)
        .collect();
    assert_eq!(
        entries,
        (1..=reply.imported_messages).collect::<Vec<_>>(),
        "{:?}",
        reply.moves.rows
    );
    for row in &reply.moves.rows {
        assert_eq!(
            row.added,
            broca_session_store::session::is_broca_id_form(&row.mid),
            "{row:?}"
        );
        assert!(!row.added || row.entry.is_some(), "{row:?}");
    }
}

/// OpenCode 2.2.3's real export imports whole: its move list, its newest message, its parts
/// numbered from `tag_floor`, and every side-file send as reads serve broca's own: the ride's
/// delivered text on its call, the second turn's client message id and marks, and the note's
/// template as its notice.
#[tokio::test]
async fn thalamus_opencode2_export_imports_into_the_store() {
    let (reply, session, export) = import_thalamus(
        "opencode2",
        "export.json",
        ImportHarness::OpenCode2,
        "ses_a0ba61c714d844b0af1d9c52d4135091",
    )
    .await;
    // The tool results split off the two assistant rows that called tools are rows of their
    // own, broca's, right after the row each was split from.
    every_ordinal_once(&reply);
    let added: Vec<(u64, &str)> = reply
        .moves
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.added)
        .map(|(at, row)| (row.entry.unwrap(), reply.moves.rows[at - 1].mid.as_str()))
        .collect();
    assert_eq!(
        added,
        [
            (3, "msg_1b49586f1001rVgZgzWdeKZmu8"),
            (12, "msg_1b495957b001zk2Vzqz0lhIKBJ")
        ]
    );
    let rows: Vec<_> = reply
        .moves
        .rows
        .iter()
        .filter(|row| !row.added)
        .map(|row| (row.mid.as_str(), row.entry))
        .collect();
    assert_eq!(
        rows,
        [
            ("msg_1b49586aa0016EUiZetDheU1f4", Some(1)),
            ("msg_1b49586f1001rVgZgzWdeKZmu8", Some(2)),
            ("msg_1b49587f10010uHhgvansXbRF1", Some(4)),
            ("msg_1b495900c001DES0ba9sPPASsN", Some(5)),
            ("msg_1b49591470019dpMEsZc2WOTUW", Some(6)),
            ("msg_1b495915a001xOIirlVXwIIakW", Some(7)),
            ("msg_1b4959296001mKWG88czxOfO3s", Some(8)),
            ("msg_1b49592a7001zLB2AYW13z4J7F", Some(9)),
            ("msg_1b495955f001hVi7sIosw8140K", Some(10)),
            ("msg_1b495957b001zk2Vzqz0lhIKBJ", Some(11)),
            ("msg_1b49595fe001dZGvbH2E283JZ2", Some(13)),
        ]
    );
    assert_eq!(reply.dropped_system_messages, 0);
    let entries = newest_and_floor(&reply, &session, &export).await;
    for (mid, ordinal) in rows
        .iter()
        .filter_map(|(mid, entry)| Some((*mid, (*entry)?)))
    {
        assert_eq!(session.find_message(mid).await.unwrap(), Some(ordinal));
    }

    let call = crate::store_read::display_facts(&entries[1]);
    assert_eq!(
        call.calls[0].call_key,
        "msg_1b49586f1001rVgZgzWdeKZmu8/call_probe"
    );
    assert_eq!(
        call.calls[0].call_carrier.as_deref(),
        Some("thalamus-harness")
    );
    let ride = &call.notices[0];
    assert_eq!(
        (
            ride.call_key.as_deref(),
            ride.send_id.as_deref(),
            ride.sender.as_deref(),
            ride.text.as_str()
        ),
        (
            Some("msg_1b49586f1001rVgZgzWdeKZmu8/call_probe"),
            Some("ride"),
            Some("magic-context"),
            "RIDE_TEXT"
        )
    );
    let first = crate::store_read::display_facts(&entries[0]);
    assert_eq!(first.sender.as_deref(), Some("prefrontal-core"));
    assert_eq!(entries[0].state.facts.as_ref().unwrap().notice, None);
    let second = crate::store_read::display_facts(&entries[5]);
    assert_eq!(second.client_message_id.as_deref(), Some("cmid-2"));
    assert_eq!(
        second.speaks_for,
        Some(serde_json::json!({"agent_id": "agent-7"}))
    );
    let note = &entries[7];
    let notice = note.state.facts.as_ref().unwrap().notice.clone().unwrap();
    assert_eq!(
        (notice.sender.as_str(), notice.template.as_deref()),
        ("magic-context", Some("note"))
    );
    assert!(crate::store_read::display_facts(note).notice);
    assert_eq!(
        note.state.facts.as_ref().unwrap().delivery_mode.as_deref(),
        Some("idle_start")
    );
    assert_eq!(
        entries[3]
            .state
            .facts
            .as_ref()
            .unwrap()
            .delivery_mode
            .as_deref(),
        Some("steer")
    );
}

/// Pi 1.5.0's real journal imports whole: its system entry dropped and listed with no entry,
/// its newest message, its parts numbered from `tag_floor`, its call keyed
/// `<entry id>/<toolCall id>` as Thalamus's runner keyed it, the reminder delivered with that
/// call's result, and the note's template as its notice.
#[tokio::test]
async fn thalamus_pi_export_imports_into_the_store() {
    let (reply, session, export) = import_thalamus(
        "pi",
        "export.jsonl",
        ImportHarness::Pi,
        "01a1b49a-6d6e-7506-bfaa-62da1e6cef3d",
    )
    .await;
    let ids = [
        "213ff4e5", "c62f7305", "1a089c7d", "daee0155", "b951b5cb", "62c953eb", "07b9345b",
        "a6909356", "93de564a", "5a2aaef2", "9c4dddb3", "fd58b6d7",
    ];
    let rows: Vec<_> = reply
        .moves
        .rows
        .iter()
        .map(|row| (row.mid.as_str(), row.entry))
        .collect();
    let mut expected = vec![("9d881624", None)];
    expected.extend(
        ids.iter()
            .enumerate()
            .map(|(at, id)| (*id, Some(at as u64 + 1))),
    );
    assert_eq!(rows, expected);
    // Pi closes no call of this session, so broca adds no message.
    every_ordinal_once(&reply);
    assert!(reply.moves.rows.iter().all(|row| !row.added));
    assert_eq!(reply.dropped_system_messages, 1);
    let entries = newest_and_floor(&reply, &session, &export).await;
    for (at, id) in ids.iter().enumerate() {
        assert_eq!(session.find_message(id).await.unwrap(), Some(at as u64 + 1));
    }

    let call = crate::store_read::display_facts(&entries[1]);
    assert_eq!(call.calls[0].call_key, "c62f7305/call_probe_1");
    assert_eq!(call.calls[0].call_carrier.as_deref(), Some("thalamus-pi"));
    let ride = &call.notices[0];
    assert_eq!(
        (
            ride.call_key.as_deref(),
            ride.send_id.as_deref(),
            ride.sender.as_deref(),
            ride.text.as_str()
        ),
        (
            Some("c62f7305/call_probe_1"),
            Some("ride"),
            Some("notifier"),
            "RIDE_REMINDER"
        )
    );
    let note = &entries[6];
    let notice = note.state.facts.as_ref().unwrap().notice.clone().unwrap();
    assert_eq!(
        (notice.sender.as_str(), notice.template.as_deref()),
        ("notifier", Some("note"))
    );
    assert!(crate::store_read::display_facts(note).notice);
    let second = crate::store_read::display_facts(&entries[4]);
    assert_eq!(second.client_message_id.as_deref(), Some("cmid-2"));
    assert_eq!(
        entries[10]
            .state
            .facts
            .as_ref()
            .unwrap()
            .delivery_mode
            .as_deref(),
        Some("steer")
    );
}

/// A Pi call left without a result is closed by the results Pi itself gives it (`No result
/// provided`), a message broca adds under its own id. The move list lists it right after the
/// assistant row whose call it closes, so the list names every ordinal once.
#[tokio::test]
async fn a_pi_call_left_unanswered_is_listed_after_its_turn() {
    let message = |id: &str, parent: Option<&str>, message: serde_json::Value| {
        serde_json::json!({"type": "message", "id": id, "parentId": parent,
                           "timestamp": "2026-01-01T00:00:01Z", "message": message})
    };
    let mut journal = String::from(
        "{\"type\":\"session\",\"id\":\"s\",\"timestamp\":\"2026-01-01T00:00:00Z\"}\n",
    );
    for entry in [
        message(
            "u1",
            None,
            serde_json::json!({"role": "user", "content": "first"}),
        ),
        message(
            "a1",
            Some("u1"),
            serde_json::json!({"role": "assistant", "stopReason": "toolUse",
                "content": [{"type": "toolCall", "id": "t1", "name": "read", "arguments": {}}]}),
        ),
        message(
            "u2",
            Some("a1"),
            serde_json::json!({"role": "user", "content": "second"}),
        ),
    ] {
        journal.push_str(&entry.to_string());
        journal.push('\n');
    }
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("pi-unanswered", journal.into_bytes(), None);
    let mut params = source.params("imp-pi-unanswered");
    params.source.harness = ImportHarness::Pi;
    params.source.session = "s".into();
    let (commands, served) = serve(create(backend).await.unwrap());
    let reply = imported(ask(&commands, source.import(params)).await);
    drop(commands);
    served.await.unwrap();
    every_ordinal_once(&reply);
    let rows: Vec<(&str, Option<u64>, bool)> = reply
        .moves
        .rows
        .iter()
        .map(|row| {
            (
                if row.added { "added" } else { row.mid.as_str() },
                row.entry,
                row.added,
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("u1", Some(1), false),
            ("a1", Some(2), false),
            ("added", Some(3), true),
            ("u2", Some(4), false),
        ]
    );
    let session = load(backend).await.unwrap();
    let closing = &session.entries(3, 1).await.unwrap()[0];
    assert_eq!(
        closing.state.facts.as_ref().unwrap().mid,
        reply.moves.rows[2].mid,
        "the added row names the closing results' own id"
    );
    assert!(matches!(
        &closing.message.content[0],
        ContentBlock::ToolResult(result) if result.is_error
    ));
}

/// An imported session states no reading of its fill until its own first step is measured. The
/// last reading it holds is the prompt size the source runner reported, for a prompt broca never
/// built, so every compaction call before that step, its Setup included, carries neither
/// `last_prompt_tokens` nor `last_prompt_view`. The session's own first reading names the view its request carried, here
/// none (`0`).
#[tokio::test]
async fn an_imported_session_states_no_reading_until_its_own_step() {
    let mut export: serde_json::Value = serde_json::from_slice(&exchanges()).unwrap();
    // The source runner reported the last answer's prompt size.
    export["messages"][7]["tokens"] = serde_json::json!({"input": 5_000, "output": 30});
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let source = Source::of("measured", serde_json::to_vec(&export).unwrap(), None);
    let (commands, served) = serve(create(backend).await.unwrap());
    imported(ask(&commands, source.import(source.params("imp-1"))).await);
    drop(commands);
    let mut session = served.await.unwrap();
    let compactor = Compactor::default();
    let mut parts = MemoryParts::default();
    run_once(
        &mut session,
        &mut parts,
        &compactor,
        ("r1", "m"),
        "Question 5.",
    )
    .await;
    // Setup counts: it comes before the step and must not carry the source's reading either. No
    // notice goes between Setup and the step, since Setup covers the imported window
    // (compaction N6), so Setup is the only call before it.
    let calls = compactor.calls.lock().unwrap();
    let readings: Vec<(&str, Option<u64>, Option<u64>)> = calls
        .iter()
        .map(|(method, params)| {
            if method == "compaction.setup" {
                assert!(
                    !params.to_string().contains("last_prompt"),
                    "Setup states a reading: {params}"
                );
            } else {
                assert!(params["conditions"].is_object(), "{method}: {params}");
            }
            let conditions = &params["conditions"];
            (
                method.as_str(),
                conditions["last_prompt_tokens"].as_u64(),
                conditions["last_prompt_view"].as_u64(),
            )
        })
        .collect();
    let own = readings
        .iter()
        .position(|(_, tokens, _)| *tokens == Some(100))
        .unwrap_or_else(|| panic!("a call reports the session's own step: {readings:?}"));
    assert!(own > 0, "{readings:?}");
    assert!(
        readings[..own]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (None, None)),
        "{readings:?}"
    );
    assert!(
        readings[own..]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (Some(100), Some(0))),
        "{readings:?}"
    );
    // The source runner's number is never stated, on any call's bytes.
    assert!(
        calls
            .iter()
            .all(|(_, params)| !params.to_string().contains("\"last_prompt_tokens\":5000")),
        "{calls:?}"
    );
}

/// `run.result` of an import's seeded run. The run ends completed and places none of its own
/// messages: imported messages belong to no run. Today's storage serves the last assistant message
/// of the transcript the run left, so the session store must serve it too, with the id the source
/// gave it, and not refuse the run as one that placed nothing. An import holding no assistant
/// message has no final message to serve, and the search for one stays inside the run's own
/// messages.
#[tokio::test]
async fn an_imported_runs_result_serves_the_last_imported_reply() {
    async fn result_of(source: &Source) -> Result<broca_wire::RunResultResponse, String> {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let (commands, served) = serve(create(backend).await.unwrap());
        imported(ask(&commands, source.import(source.params("imp-1"))).await);
        drop(commands);
        served.await.unwrap();
        let session = load(backend).await.unwrap();
        let run_id = session.operations.owner.first_start.clone().unwrap().run_id;
        let parts = MemoryParts::default();
        let reader = crate::store_status::Reader {
            labels: Default::default(),
            parts: &parts,
            live: false,
            settled: None,
            session_name: "alfonso:head:a1",
            live_step: None,
        };
        reader
            .run_result(&session, &broca_protocol::RunId(run_id))
            .await
            .map_err(|error| format!("{error:?}"))
    }

    let result = result_of(&Source::new("final")).await.unwrap();
    assert_eq!(result.state, "completed");
    let message = result
        .final_message
        .expect("a completed run's final message");
    assert_eq!(message.ordinal, 4);
    assert_eq!(message.mid, "msg_a2");
    assert_eq!(message.text, "Done.");

    let prompt_only = serde_json::to_vec(&serde_json::json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look."}
        ]
    }))
    .unwrap();
    let refused = result_of(&Source::of("prompt-only", prompt_only, None))
        .await
        .unwrap_err();
    assert!(
        refused.contains("the completed run has no assistant message"),
        "{refused}"
    );
}
