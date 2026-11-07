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
}

impl Source {
    fn new(name: &str) -> Self {
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
        let source = Self { dir, roots };
        source.write();
        source
    }

    fn path(&self) -> PathBuf {
        self.dir.join("head.json")
    }

    fn write(&self) {
        std::fs::write(self.path(), export()).unwrap();
        std::fs::write(self.dir.join("head.sends.json"), sends()).unwrap();
    }

    fn params(&self, import_id: &str) -> FileImportParams {
        FileImportParams {
            import_id: import_id.into(),
            source: ImportSource {
                harness: ImportHarness::OpenCode2,
                path: self.path().display().to_string(),
                session: "ses_head".into(),
                sha256: format!("{:x}", sha2::Sha256::digest(export())),
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
    let page = crate::store_read::read_page(&session, Some(0), Some(10))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 4);
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
