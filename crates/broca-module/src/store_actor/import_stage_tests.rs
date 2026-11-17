//! An import written in batches into a staged session and published whole over the empty
//! session it was admitted into, on the new storage's class roots: an import larger than one log
//! batch, parity with the single batch an import was written as before staging, and a crash at
//! each step before and after the publication, each reopened through the new storage's own open.
//! Every case runs in both cipher modes.

use std::path::Path;
use std::time::Duration;

use broca_core::FileLeaseStore;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::keys::Cipher;
use broca_session_store::record::Placement;
use broca_session_store::session::MAX_BATCH_RECORDS;
use broca_wal::StorageClass;
use serde_json::{json, Value};

use super::*;
use crate::store_import::{read_source, seed, SeedFacts};
use crate::store_import_stage::{import_staging_root, FsImportStaging, ImportCut};
use crate::store_open::StoreRuntime;

const IDLE: Duration = Duration::from_millis(100);
const CIPHERS: [Cipher; 2] = [Cipher::XChaCha20Poly1305, Cipher::None];

fn deployment_in(cipher: Cipher) -> DeploymentKey {
    deployment().creating(cipher)
}

fn address() -> SessionAddr {
    SessionAddr(broca_core::session_addr(&bind()))
}

/// A module over the state root `dir`, whose sessions the registry opens as in production.
struct Module {
    runtime: Arc<StoreRuntime>,
    registry: crate::registry::LineageRegistry,
}

async fn module(dir: &Path, cipher: Cipher) -> Module {
    std::fs::create_dir_all(dir).unwrap();
    let executor: Arc<dyn crate::executor::RunExecutor> = Arc::new(executor(Vec::new(), None));
    let runtime = Arc::new(
        StoreRuntime::open(dir, deployment_in(cipher), Arc::clone(&executor), IDLE)
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
    Module { runtime, registry }
}

impl Module {
    async fn import(&self, source: &Source, import_id: &str) -> StoreOutcome {
        self.registry
            .route_store_command(&bind(), source.import(source.params(import_id)))
            .await
            .unwrap()
    }

    async fn view(&self) -> Arc<crate::store_read::StoredView<LocalBackend>> {
        self.runtime
            .current_view(&bind())
            .await
            .unwrap()
            .expect("the session reads")
    }
}

/// A head of `exchanges` question-and-answer pairs imports whole though its records fill more
/// than one log batch, and the session reads back what the source held.
#[tokio::test]
async fn an_import_larger_than_one_log_batch_is_published_whole() {
    const EXCHANGES: u64 = 17_000;
    for cipher in CIPHERS {
        let source = Source::of("large", many_exchanges(EXCHANGES), None);
        let module = module(&source.dir.join("state"), cipher).await;
        let reply = imported(module.import(&source, "imp-1").await);
        assert_eq!(reply.imported_messages, 2 * EXCHANGES, "{cipher:?}");
        let view = module.view().await;
        let records = view.records_between(1, view.next_seq() - 1).await.unwrap();
        let start = records
            .iter()
            .position(|(_, record)| matches!(record, Record::RunStart(_)))
            .expect("the seeded run");
        let end = records
            .iter()
            .position(|(_, record)| matches!(record, Record::RunEnd(_)))
            .expect("the seeded run's end");
        assert!(
            end - start + 1 > MAX_BATCH_RECORDS,
            "{cipher:?}: the import fills more than one batch"
        );
        let imported_inputs = records[start..=end]
            .iter()
            .filter(|(_, record)| {
                matches!(record, Record::Input(input) if input.placement == Placement::Imported)
            })
            .count();
        assert_eq!(imported_inputs as u64, 2 * EXCHANGES, "{cipher:?}");
        assert_eq!(view.conversation.placed, 2 * EXCHANGES, "{cipher:?}");
        for n in [1, EXCHANGES / 2, EXCHANGES] {
            assert_eq!(
                view.find_message(&format!("msg_u{n}")).await.unwrap(),
                Some(2 * n - 1),
                "{cipher:?}"
            );
            assert_eq!(
                view.find_message(&format!("msg_a{n}")).await.unwrap(),
                Some(2 * n),
                "{cipher:?}"
            );
            let entries = view.entries(2 * n - 1, 2).await.unwrap();
            let texts: Vec<String> = entries
                .iter()
                .map(|entry| {
                    entry
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text { text } => Some(text.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect();
            assert_eq!(
                texts,
                [
                    format!("Question {n}."),
                    format!("Answer {n} first part. Answer {n} second part.")
                ],
                "{cipher:?}"
            );
        }
        let retry = imported(module.import(&source, "imp-1").await);
        assert_eq!(retry, reply, "{cipher:?}");
        assert!(module.registry.stop_and_seal().await);
    }
}

/// Replaces in `value` each string's `needles` by their placeholders, and each cursor and digest
/// by whether it is present: what two sessions mint for themselves (ids, incarnation, and the
/// keys cursors and a head's digest are made with) differs even where their records agree.
fn masked(mut value: Value, needles: &[(String, &str)]) -> Value {
    fn walk(value: &mut Value, needles: &[(String, &str)]) {
        match value {
            Value::String(text) => {
                for (needle, placeholder) in needles {
                    *text = text.replace(needle.as_str(), placeholder);
                }
            }
            Value::Object(members) => {
                for (key, member) in members.iter_mut() {
                    if key.contains("cursor") || key == "digest" {
                        *member = json!(!member.is_null());
                    } else {
                        walk(member, needles);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| walk(item, needles)),
            _ => {}
        }
    }
    walk(&mut value, needles);
    value
}

/// What one side of the parity test reads of its imported session, with what the session mints
/// for itself replaced by placeholders.
#[derive(Debug, PartialEq)]
struct Read {
    /// The seqs of the import's records, first and last.
    seqs: (u64, u64),
    records: Vec<String>,
    receipt: String,
    /// The ordinal each message the receipt lists resolves to.
    ids: Vec<(String, Option<u64>)>,
    reply: Value,
    read: Value,
    history: Value,
    entries: String,
}

async fn read_back(module: &Module, reply: &FileImportReply) -> Read {
    let view = module.view().await;
    let records = view.records_between(1, view.next_seq() - 1).await.unwrap();
    let (first, pointer) = records
        .iter()
        .find_map(|(seq, record)| match record {
            Record::RunStart(start) => start.imported_ids.clone().map(|ids| (*seq, ids)),
            _ => None,
        })
        .expect("the seeded run names its id map");
    assert_eq!(pointer.seq, first);
    let prefix = format!(
        "g{}",
        &broca_session_store::codec::hex(&pointer.generation.0)[..16]
    );
    let incarnation = broca_session_store::codec::hex(&view.head().incarnation.0);
    let needles = [
        (format!("{:?}", pointer.log), "<log>"),
        (format!("{:?}", pointer.generation), "<generation>"),
        (broca_session_store::codec::hex(&pointer.log.0), "<log>"),
        (prefix.clone(), "<prefix>"),
        (incarnation, "<incarnation>"),
    ];
    let mask_text = |text: String| {
        needles.iter().fold(text, |text, (needle, placeholder)| {
            text.replace(needle.as_str(), placeholder)
        })
    };
    let last = records
        .iter()
        .find_map(|(seq, record)| matches!(record, Record::RunEnd(_)).then_some(*seq))
        .expect("the seeded run's end");
    let records: Vec<String> = records
        .into_iter()
        .filter(|(seq, _)| (first..=last).contains(seq))
        .map(|(seq, mut record)| {
            if let Record::RunStart(start) = &mut record {
                start.imported_ids = None;
            }
            mask_text(format!("{seq} {record:?}"))
        })
        .collect();
    let receipt = view.import_receipt().await.unwrap().expect("a receipt");
    let mut ids = Vec::new();
    for row in &receipt.moves.clone() {
        ids.push((
            mask_text(row.mid.clone()),
            view.find_message(&row.mid).await.unwrap(),
        ));
    }
    let receipt = mask_text(format!("{receipt:?}"));
    use crate::serve::{ServeHandler, SessionReadRequest};
    let read = module
        .registry
        .read(SessionReadRequest::new(bind(), None, Some(100)))
        .await
        .expect("session.read");
    let history = crate::history::read(
        &module.registry,
        &bind(),
        &serde_json::from_value(json!({})).unwrap(),
    )
    .await
    .expect("session.history");
    let entries = module
        .runtime
        .read(
            &module.registry,
            &bind(),
            crate::store_reads::StoreReadRequest::Entries {
                from: 1,
                limit: 100,
            },
        )
        .await
        .unwrap()
        .expect("session.entries");
    let crate::store_reads::StoreReadReply::Entries(Ok(entries)) = entries else {
        panic!("session.entries refused")
    };
    Read {
        seqs: (first, last),
        records,
        receipt,
        ids,
        reply: masked(serde_json::to_value(reply).unwrap(), &needles),
        read: masked(serde_json::to_value(&read).unwrap(), &needles),
        history: masked(history, &needles),
        entries: mask_text(format!("{entries:?}")),
    }
}

/// An import small enough for one log batch, written through staging and published, reads
/// back exactly as the same import appended in one batch to the session as created, which is
/// how every import was written before staging: the same records at the same seqs, the same
/// receipt and id map, and the same `session.read`, `session.history` and `session.entries`.
#[tokio::test]
async fn a_staged_import_reads_as_the_same_import_written_in_one_batch() {
    for cipher in CIPHERS {
        let source = Source::new("parity");
        // The import as it was written before staging: seeded from the session as created and
        // appended to it in one batch.
        let single = module(&source.dir.join("single"), cipher).await;
        {
            let mut session = Session::create(
                LocalBackend::new(StorageClass::Durable.store_root(&source.dir.join("single"))),
                address(),
                crate::store_open::triple(&bind()),
                single.runtime.deployment(),
                false,
            )
            .await
            .unwrap();
            let params = source.params("imp-1");
            let (_, history) = read_source(&params, &source.roots).unwrap();
            let prefix = session.message_id_prefix();
            let seed = seed(
                history,
                &SeedFacts {
                    params: &params,
                    principal: &core(),
                    scope: Some(&scope()),
                    now_ms: 9_000,
                    prefix: &prefix,
                    first_seq: session.next_seq(),
                },
            )
            .unwrap();
            assert!(seed.records.len() <= MAX_BATCH_RECORDS);
            session
                .append_import(&seed.records, seed.media, seed.ids, &seed.receipt)
                .await
                .unwrap();
            session.close().await.unwrap();
        }
        // Its retry answers from the receipt, as any import's does.
        let single_reply = imported(single.import(&source, "imp-1").await);

        let staged = module(&source.dir.join("staged"), cipher).await;
        let staged_reply = imported(staged.import(&source, "imp-1").await);

        let single = read_back(&single, &single_reply).await;
        let staged = read_back(&staged, &staged_reply).await;
        assert_eq!(staged.seqs, single.seqs, "{cipher:?}: seqs");
        assert_eq!(staged.records, single.records, "{cipher:?}: records");
        assert_eq!(staged.receipt, single.receipt, "{cipher:?}: receipt");
        assert_eq!(staged.ids, single.ids, "{cipher:?}: id map");
        assert_eq!(staged.reply, single.reply, "{cipher:?}: reply");
        assert_eq!(staged.read, single.read, "{cipher:?}: session.read");
        assert_eq!(
            staged.history, single.history,
            "{cipher:?}: session.history"
        );
        assert_eq!(
            staged.entries, single.entries,
            "{cipher:?}: session.entries"
        );
    }
}

/// A crash at each step of an import, then the new storage opened again as at a restart: before
/// the head is replaced the session is the empty one the import was admitted into, whatever the
/// attempt staged is gone, and a retry under the same import id is admitted as new; after it the
/// session is the whole import, and the retry is answered from its receipt. The import here
/// appends four records a batch, so it takes several.
#[tokio::test]
async fn a_crash_at_any_step_of_a_staged_import_leaves_the_empty_session_or_the_whole_import() {
    let cuts = [
        ("before_first_batch", false),
        ("between_batches", false),
        ("after_last_batch", false),
        // The staged log is renamed beside the empty session's, its objects not yet.
        ("rename_logs", false),
        ("after_publish", true),
    ];
    for cipher in CIPHERS {
        for (at, published) in cuts {
            let label = format!("{cipher:?} {at}");
            let source = Source::new("crash");
            let dir = source.dir.join("state");
            std::fs::create_dir_all(&dir).unwrap();
            {
                let runtime = StoreRuntime::open(
                    &dir,
                    deployment_in(cipher),
                    Arc::new(executor(Vec::new(), None)),
                    IDLE,
                )
                .await
                .unwrap();
                // The session as the registry creates it for an import.
                let store = StorageClass::Durable.store_root(&dir);
                let session = Session::create(
                    LocalBackend::new(&store),
                    address(),
                    crate::store_open::triple(&bind()),
                    runtime.deployment(),
                    false,
                )
                .await
                .unwrap();
                let cut = ImportCut::new(at);
                let mut staging = FsImportStaging::new(&dir, store, runtime.deployment_shared());
                staging.cut = Some(cut.clone());
                let actor = StoreActor::new(
                    bind(),
                    session,
                    Arc::new(executor(Vec::new(), None)),
                    Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
                )
                .with_import_staging(Arc::new(staging))
                .with_import_cut(4, Some(cut.clone()));
                let (commands, receiver) = mpsc::unbounded_channel();
                let served = tokio::spawn(actor.serve(receiver));
                let (reply, _answer) = oneshot::channel();
                commands
                    .send(LineageCmd::Store {
                        command: source.import(source.params("imp-1")),
                        reply,
                    })
                    .unwrap();
                cut.reached.notified().await;
                // The process dies here: nothing after the cut runs.
                served.abort();
                assert!(
                    matches!(served.await, Err(error) if error.is_cancelled()),
                    "{label}"
                );
            }
            assert!(
                import_staging_root(&dir).exists(),
                "{label}: the attempt staged"
            );

            let module = module(&dir, cipher).await;
            assert!(
                !import_staging_root(&dir).exists(),
                "{label}: the new storage's open removes what the attempt staged"
            );
            let view = module.view().await;
            if published {
                assert_eq!(view.conversation.placed, 4, "{label}");
                assert!(view.import_receipt().await.unwrap().is_some(), "{label}");
                // The empty session's log is listed for deletion once its grace has passed.
                assert_eq!(view.head().pending.len(), 1, "{label}");
                assert_eq!(view.head().pending[0].logs.len(), 1, "{label}");
            } else {
                assert_eq!(view.conversation.placed, 0, "{label}");
                assert!(view.operations.run.is_none(), "{label}");
                assert!(view.operations.imported_ids.is_none(), "{label}");
            }
            drop(view);
            let retry = imported(module.import(&source, "imp-1").await);
            assert_eq!(retry.imported_messages, 4, "{label}");
            assert_eq!(
                moved(&retry.moves),
                [
                    ("msg_u1".to_owned(), Some(1), false),
                    ("msg_sys".to_owned(), None, false),
                    ("msg_a1".to_owned(), Some(2), false),
                    ("added".to_owned(), Some(3), true),
                    ("msg_a2".to_owned(), Some(4), false)
                ],
                "{label}"
            );
            assert_eq!(module.view().await.conversation.placed, 4, "{label}");
            assert!(module.registry.stop_and_seal().await, "{label}");
        }
    }
}
