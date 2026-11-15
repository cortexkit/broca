//! `session.media_read` and the upload operations on a session kept in the new storage, through
//! the module's registry.
//!
//! The session got its image from a head import, which stores it as an input medium of the
//! imported message. Each reply is pinned (see `broca_test_support::pins`).

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_protocol::{BindIdentity, BindPrincipal, NormalizedEvent, RunId, SubmissionRequest};
use broca_session_store::keys::Cipher;
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::mpsc;

use super::{core, head_scope, request};
use crate::actor::{LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunJob};
use crate::registry::LineageRegistry;
use crate::serve::ServeHandler;

cipher_tests!(
    uploads_and_media_reads_answer_alike,
    a_store_sessions_media_survives_a_restart_and_stays_its_own,
);

/// A one-pixel PNG: the image both sessions hold.
pub(super) const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
/// A three-by-two PNG: uploaded to each session and never sent.
pub(super) const WIDE: &str = "89504e470d0a1a0a0000000d49484452000000030000000208020000001216f14d0000001049444154789c63f8cfc000410c70160041d205fb87f0b9480000000049454e44ae426082";

pub(super) fn bytes_of(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}

pub(super) fn base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn stranger() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    }
}

/// The mode the stored head of the session `key` under the state root `dir` names, read from
/// whichever root holds it (an imported session is durable, a sent one ordinary).
pub(super) fn stored_mode(dir: &std::path::Path, key: &BindIdentity) -> Cipher {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    let address = broca_core::session_addr(key);
    let class = broca_wal::session_storage_class(dir, &address)
        .unwrap()
        .expect("the session is kept in the new storage");
    let (stored, _) = LocalBackend::new(class.store_root(dir))
        .read_stored_head(&SessionAddr(address))
        .unwrap()
        .expect("the session is kept in the new storage");
    broca_session_store::head::stored_cipher(&stored).unwrap()
}

/// The media profile of a model that takes images.
pub(super) fn image_profile() -> broca_provider::media_profile::MediaProfile {
    broca_provider::media_profile::select_media_profile(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        Some(broca_provider::media_profile::MediaQuirk::Anthropic),
        "m",
        broca_provider::spec::InputModalities::from_value(&json!(["text", "image"])),
        Some(200_000),
    )
}

/// The frozen render config of a model that takes images, as its provider freezes it.
pub(super) fn image_render_config() -> Value {
    let mut config = broca_provider::families::frozen_config_for_family(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        "m",
    );
    config.media = Some(image_profile());
    serde_json::to_value(config).unwrap()
}

/// The module's executor in these scenarios: the new loop, with the media profile of a model
/// that takes images.
struct Executor {
    store: crate::store_actor::tests::LoopExecutor,
}

#[async_trait]
impl RunExecutor for Executor {
    async fn media_profile(
        &self,
        _request: &SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(Some(Box::new(image_profile())))
    }

    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.store.execute_on_store(job, out).await
    }
}

/// A module over a scratch folder, which it removes when dropped.
pub(super) struct Side {
    pub(super) dir: std::path::PathBuf,
    pub(super) registry: LineageRegistry,
    pub(super) key: BindIdentity,
    /// What each model step waits for, when the side's steps are gated.
    pub(super) gate: Option<Arc<tokio::sync::Notify>>,
    /// The mode the side's module creates new-storage sessions in.
    pub(super) cipher: Cipher,
    /// The caller whose sends reach the side's session.
    pub(super) caller: crate::store_actor::tests::Caller,
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A module whose sessions start in the new storage when `store`, over the scripted model
/// `steps`. Its new storage creates sessions in mode `cipher`.
pub(super) async fn side(store: bool, steps: Vec<Vec<NormalizedEvent>>, cipher: Cipher) -> Side {
    side_gated(store, steps, false, cipher).await
}

/// [`side`], with every model step waiting for the side's gate when `gated`.
pub(super) async fn side_gated(
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    cipher: Cipher,
) -> Side {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-media-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let gate = gated.then(|| Arc::new(tokio::sync::Notify::new()));
    let registry = registry_gated(&dir, store, steps, gate.clone(), cipher).await;
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Side {
        dir,
        registry,
        key,
        gate,
        cipher,
        caller: Default::default(),
    }
}

/// A module over the state root `dir`, as a process starting there opens it: its sessions
/// start in the new storage when `store`, its runs scripted by `steps`, and its new storage
/// creates sessions in mode `cipher`.
pub(super) async fn registry_at(
    dir: &std::path::Path,
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: Cipher,
) -> LineageRegistry {
    registry_gated(dir, store, steps, None, cipher).await
}

/// [`registry_at`], with every model step waiting for `gate` when there is one.
async fn registry_gated(
    dir: &std::path::Path,
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<tokio::sync::Notify>>,
    cipher: Cipher,
) -> LineageRegistry {
    broca_core::media_store::create_media_directories(dir).unwrap();
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    // The runs freeze the media profile of a model that takes images.
    let mut store_executor = crate::store_actor::tests::executor(steps, gate);
    store_executor.provider.render_config = Some(image_render_config());
    let executor: Arc<dyn RunExecutor> = Arc::new(Executor {
        store: store_executor,
    });
    let runtime = crate::store_open::StoreRuntime::open(
        dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
        Arc::clone(&executor),
        super::IDLE,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(super::IDLE)
            .with_store_runtime(Arc::new(runtime)),
        ),
        wal,
        None,
    );
    if store {
        registry.with_store_new_sessions()
    } else {
        registry
    }
}

impl Side {
    /// The mode the stored head of the side's session names, which a session in the new storage
    /// keeps for its life.
    pub(super) fn stored_mode(&self) -> Cipher {
        stored_mode(&self.dir, &self.key)
    }

    pub(super) async fn send(&self, run_id: &str, request: SubmissionRequest) -> SendOutcome {
        self.deliver(run_id, request, broca_protocol::Delivery::Queue)
            .await
    }

    /// A send asking to be delivered as `delivery`, as the side's caller sends it.
    pub(super) async fn deliver(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        delivery: broca_protocol::Delivery,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &SendKey::Minted, request),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                delivery,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    pub(super) async fn idle(&self) {
        broca_test_support::wait_until("the session is idle", || async {
            match self.registry.route_status(&self.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    /// Lets the side's gated model steps through, one at a time, until its session is idle.
    pub(super) async fn release_to_idle(&self) {
        broca_test_support::wait_until("the session is idle", || async {
            if let Some(gate) = &self.gate {
                gate.notify_one();
            }
            match self.registry.route_status(&self.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    /// An upload operation, by `principal` on the session's own head scope, as JSON or its
    /// refusal.
    pub(super) async fn upload(
        &self,
        principal: &BindPrincipal,
        method: &str,
        params: Value,
    ) -> Result<Value, String> {
        ServeHandler::upload(
            &self.registry,
            &self.key,
            Some(principal),
            &head_scope(core()),
            method,
            params,
        )
        .await
        .map_err(|error| format!("{error:?}"))
    }

    /// Uploads `bytes` as core, committed; the upload's id.
    pub(super) async fn uploaded(&self, bytes: &[u8]) -> String {
        let begin = self
            .upload(
                &core(),
                "session.upload_begin",
                json!({"mime": "image/png", "bytes": bytes.len(), "sha256": digest(bytes)}),
            )
            .await
            .unwrap();
        let id = begin["upload_id"].as_str().unwrap().to_owned();
        self.upload(
            &core(),
            "session.upload_append",
            json!({"upload_id": id, "offset": 0, "data": base64(bytes)}),
        )
        .await
        .unwrap();
        self.upload(&core(), "session.upload_commit", json!({"upload_id": id}))
            .await
            .unwrap();
        id
    }

    pub(super) async fn media_read(&self, params: Value) -> Result<Value, String> {
        ServeHandler::media_read(&self.registry, &self.key, params)
            .await
            .map_err(|error| format!("{error:?}"))
    }
}

/// A photo send: a text message by core under its head scope, with the uploads `upload_ids`
/// attached.
pub(super) fn photo(text: &str, upload_ids: &[&str]) -> SubmissionRequest {
    let mut sent = request(text);
    super::scoped_by_core(&mut sent);
    sent.content = serde_json::from_value(Value::Array(
        upload_ids
            .iter()
            .map(|id| json!({"type": "upload", "upload_id": id}))
            .collect(),
    ))
    .unwrap();
    sent
}

/// The head scope core's sends freeze, as an import takes it.
fn session_scope() -> broca_protocol::SessionScope {
    match head_scope(core()) {
        broca_protocol::SendScope::Stamped(stamped) => stamped.scope,
        _ => unreachable!("a stamped scope"),
    }
}

/// Imports into the session `key` of `registry`, over the state root `dir`, a head whose first
/// message carries `image`, owned by core under its head scope.
async fn import_into(
    registry: &LineageRegistry,
    dir: &std::path::Path,
    key: &BindIdentity,
    image: &[u8],
) {
    use crate::store_import::{ImportCommand, ImportRoots, StoreCommand, StoreOutcome};
    let folder = dir.join("imports").join(&key.session);
    std::fs::create_dir_all(&folder).unwrap();
    let export = serde_json::to_vec(&json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look.",
             "files": [{"mime": "image/png", "data": base64(image)}]},
            {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
             "content": [{"type": "text", "text": "Seen."}]}
        ]
    }))
    .unwrap();
    let path = folder.join("head.json");
    std::fs::write(&path, &export).unwrap();
    let params = serde_json::from_value(json!({
        "import_id": "imp-1",
        "source": {
            "harness": "opencode2",
            "path": path.display().to_string(),
            "session": "ses_head",
            "sha256": digest(&export),
        },
        "origin": {"kind": "import", "source_ref": "thalamus:ses_head"},
    }))
    .unwrap();
    let command = StoreCommand::Import(Box::new(ImportCommand {
        params,
        principal: core(),
        scope: Some(session_scope()),
        roots: Arc::new(ImportRoots::new(vec![folder.clone()], Some(folder))),
        now_ms: 9_000,
    }));
    let outcome = registry.route_store_command(key, command).await.unwrap();
    assert!(matches!(outcome, StoreOutcome::Imported(_)), "{outcome:?}");
}

/// Imports into the store side's session a head whose first message carries `image`.
async fn import_with(side: &Side, image: &[u8]) {
    import_into(&side.registry, &side.dir, &side.key, image).await;
}

/// A reply with the upload's id, which each side mints for itself, taken out: from a reply's
/// `upload_id`, and from a refusal naming the upload `id`.
fn masked(reply: Result<Value, String>, id: Option<&str>) -> Result<Value, String> {
    match reply {
        Ok(mut value) => {
            if let Some(id) = value.get_mut("upload_id") {
                *id = json!(id.is_string());
            }
            Ok(value)
        }
        Err(error) => Err(match id {
            Some(id) => error.replace(id, "<upload>"),
            None => error,
        }),
    }
}

/// The media reads worth telling apart, of the medium with digest `sha256` and `size` bytes: the
/// whole medium, a byte range, a range reaching past its end, a metadata-only probe, a read at its
/// end, one past it, and one longer than a chunk.
pub(super) fn reads_of(sha256: &str, size: u64) -> Vec<Value> {
    vec![
        json!({"sha256": sha256, "offset": 0, "length": size}),
        json!({"sha256": sha256, "offset": 10, "length": 20}),
        json!({"sha256": sha256, "offset": size - 5, "length": 100}),
        json!({"sha256": sha256, "offset": 0, "length": 0}),
        json!({"sha256": sha256, "offset": size, "length": 10}),
        json!({"sha256": sha256, "offset": size + 1, "length": 10}),
        json!({"sha256": sha256, "offset": 0, "length": 2_097_153}),
    ]
}

/// The upload operations and `session.media_read` on a session in the new storage: an upload's
/// every step and its refusals, the image the session holds read whole, by byte range and as a
/// metadata probe, and the refusals of a digest the session doesn't hold. An upload no send named
/// is not readable, and neither is a digest of another session, nor one malformed.
async fn uploads_and_media_reads_answer_alike(cipher: Cipher) {
    let steps = || {
        vec![
            super::super::tests::text_step("a1", "hello"),
            super::super::tests::text_step("a2", "seen"),
        ]
    };
    let store = side(true, steps(), cipher).await;
    let image = bytes_of(PNG);
    let unsent = bytes_of(WIDE);

    // Before the session exists, an upload is refused.
    let begin = json!({"mime": "image/png", "bytes": unsent.len(), "sha256": digest(&unsent)});
    broca_test_support::pin!(
        "an upload before the session's first send",
        store
            .upload(&core(), "session.upload_begin", begin.clone())
            .await
    );

    // A head whose first message carries the image.
    import_with(&store, &image).await;
    assert_eq!(store.stored_mode(), cipher);

    // Every step of an upload, and its refusals.
    let new = store
        .upload(&core(), "session.upload_begin", begin.clone())
        .await;
    broca_test_support::pin!("upload_begin", masked(new.clone(), None));
    let id = new.unwrap()["upload_id"].as_str().unwrap().to_owned();
    for (method, params) in [
        ("session.upload_status", json!({"upload_id": id})),
        (
            "session.upload_append",
            json!({"upload_id": id, "offset": 3, "data": base64(&unsent)}),
        ),
        (
            "session.upload_append",
            json!({"upload_id": id, "offset": 0, "data": base64(&unsent[..10])}),
        ),
        ("session.upload_commit", json!({"upload_id": id})),
        (
            "session.upload_append",
            json!({"upload_id": id, "offset": 10, "data": base64(&unsent[10..])}),
        ),
        ("session.upload_status", json!({"upload_id": id})),
        ("session.upload_commit", json!({"upload_id": id})),
        (
            "session.upload_commit",
            json!({"upload_id": "no-such-upload"}),
        ),
        (
            "session.upload_status",
            json!({"upload_id": "no-such-upload"}),
        ),
        ("session.upload_status", json!({})),
    ] {
        let new = masked(
            store.upload(&core(), method, params.clone()).await,
            Some(&id),
        );
        broca_test_support::pin!(format!("{method} {params:?}"), new);
    }
    // Another module may not upload to core's session.
    let new = store
        .upload(&stranger(), "session.upload_begin", begin)
        .await;
    assert!(new.is_err(), "{new:?}");
    broca_test_support::pin!("another module's upload", new);

    // The session's image reads back whole, by range and as a probe; and what its records don't
    // name is refused.
    let size = image.len() as u64;
    let mut reads = reads_of(&digest(&image), size);
    reads.extend([
        json!({"sha256": digest(&unsent), "offset": 0, "length": 10}),
        json!({"sha256": digest(b"never stored"), "offset": 0, "length": 10}),
        json!({"sha256": "not-a-digest", "offset": 0, "length": 10}),
        json!({"sha256": digest(&image), "offset": 0}),
    ]);
    for params in reads {
        let new = store.media_read(params.clone()).await;
        broca_test_support::pin!(format!("media_read {params}"), new);
    }
    let whole = store
        .media_read(json!({"sha256": digest(&image), "offset": 0, "length": size}))
        .await
        .unwrap();
    assert_eq!(
        whole["data"],
        json!(base64(&image)),
        "the image reads back byte-equal"
    );
}

/// A store session's image survives a restart: a module started anew on the same state root
/// reads it back byte-equal, whole and by byte range. Another session of the module, whose
/// records don't name the image, can't read it, though the image's bytes are stored beside it.
async fn a_store_sessions_media_survives_a_restart_and_stays_its_own(cipher: Cipher) {
    let store = side(true, Vec::new(), cipher).await;
    let (image, other_image) = (bytes_of(PNG), bytes_of(WIDE));
    import_with(&store, &image).await;
    assert_eq!(store.stored_mode(), cipher);
    let other = BindIdentity::new(store.dir.join("project"), "store-actor", "s2");
    import_into(&store.registry, &store.dir, &other, &other_image).await;
    let read = json!({"sha256": digest(&image), "offset": 0, "length": image.len()});
    assert_eq!(
        store.media_read(read.clone()).await.unwrap()["data"],
        json!(base64(&image))
    );
    assert!(store.registry.stop_and_seal().await);

    let restarted = registry_at(&store.dir, true, Vec::new(), cipher).await;
    let reply = ServeHandler::media_read(&restarted, &store.key, read.clone())
        .await
        .unwrap();
    assert_eq!(reply["data"], json!(base64(&image)), "after a restart");
    assert_eq!(
        reply["size"],
        json!(image.len()),
        "the size is the image's own"
    );
    let range = ServeHandler::media_read(
        &restarted,
        &store.key,
        json!({"sha256": digest(&image), "offset": 7, "length": 9}),
    )
    .await
    .unwrap();
    assert_eq!(range["data"], json!(base64(&image[7..16])));
    // The other session names only its own image.
    let refused = ServeHandler::media_read(&restarted, &other, read)
        .await
        .unwrap_err();
    assert!(
        format!("{refused:?}").contains("unknown_media"),
        "{refused:?}"
    );
    let own = ServeHandler::media_read(
        &restarted,
        &other,
        json!({"sha256": digest(&other_image), "offset": 0, "length": other_image.len()}),
    )
    .await
    .unwrap();
    assert_eq!(own["data"], json!(base64(&other_image)));
    assert!(restarted.stop_and_seal().await);
}
