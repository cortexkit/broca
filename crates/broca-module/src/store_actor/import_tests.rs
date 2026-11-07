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

use super::tests::{executor, request, send_as, text_step};
use super::StoreActor;
use crate::actor::{LineageCmd, SendKey, SendOutcome};
use crate::store_import::{
    ImportCommand, ImportRoots, InstallViewCommand, StoreCommand, StoreOutcome,
};

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
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor(Vec::new(), None)),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    );
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
    let rows: Vec<_> = reply
        .moves
        .rows
        .iter()
        .map(|row| (row.mid.as_str(), row.entry))
        .collect();
    assert_eq!(
        rows,
        [
            ("msg_u1", Some(1)),
            ("msg_sys", None),
            ("msg_a1", Some(2)),
            ("msg_a2", Some(4))
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
    let outcome = send_as(
        &commands,
        "r1",
        request("next"),
        SendKey::Minted,
        broca_protocol::Delivery::Queue,
    )
    .await;
    assert!(
        matches!(&outcome, SendOutcome::Error(message) if message.contains("session.adopt")),
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
    let crate::store_read::StorePage { page, mids } =
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
                    assert_eq!(retry.moves, reference.moves, "cut {cut}");
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
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
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

/// Through the registry: an import creates its session in the new storage, never on today's,
/// and a retry is answered from it; with the new storage off, an import is refused by name.
#[tokio::test]
async fn the_registry_creates_an_imported_session_in_the_new_storage() {
    use broca_core::{FileLeaseStore, FileWalStore, WalStore};
    const IDLE: std::time::Duration = std::time::Duration::from_millis(100);
    let source = Source::new("registry");
    let dir = source.dir.join("state");
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
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
                wal.clone(),
                Arc::clone(&executor),
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(runtime),
        )
    };
    let key = broca_protocol::BindIdentity::new(dir.join("project"), "broca", "alfonso:head:a1");
    let off = crate::registry::LineageRegistry::new_with_reader(
        opener(Arc::clone(&runtime)),
        wal.clone(),
        None,
    );
    assert_eq!(
        refused(
            off.route_store_command(&key, source.import(source.params("imp-1")))
                .await
                .unwrap()
        ),
        "new_storage_unavailable"
    );
    assert!(!runtime.holds(&key).unwrap());
    let registry = crate::registry::LineageRegistry::new_with_reader(
        opener(Arc::clone(&runtime)),
        wal.clone(),
        None,
    )
    .with_store_new_sessions();
    let reply = imported(
        registry
            .route_store_command(&key, source.import(source.params("imp-1")))
            .await
            .unwrap(),
    );
    assert_eq!(reply.imported_messages, 4);
    assert!(runtime.holds(&key).unwrap());
    assert_eq!(wal.wal_present(&key), Some(false));
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

    fn message_id_prefix(&self) -> String {
        Writer::message_id_prefix(&*self.0)
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        Writer::open_log_records(&*self.0).await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        Writer::run_start(&*self.0, run_id).await
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
                        text: format!("Reply to {text}"),
                    },
                    ContentBlock::Text {
                        text: format!("More on {text}"),
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
    (run_id, model): (&str, &str),
    text: &str,
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
    let seams = broca_session_loop::run::Seams {
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
        compaction_plane: Some(compactor),
        compaction_session: None,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
    };
    broca_session_loop::run::run(writer, parts, admission, &seams)
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
        mids.push(entry.state.facts.unwrap().mid);
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
