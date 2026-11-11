//! A head imported into the new storage whose history holds images: its first owner run's
//! request carries them, and so does every later one, through the session's media fold and the
//! real renderer. An import writes its whole history as one seeded run, which the media fold must
//! read the way today's storage reads its import seed.

use std::sync::Arc;

use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
use broca_protocol::BindIdentity;
use broca_session_store::keys::Cipher;
use serde_json::json;

use super::super::media::{base64, bytes_of, digest, PNG, WIDE};
use super::{photo, planned, send_planned, text, text_step, Rendered};
use crate::actor::SendOutcome;
use crate::store_import::{
    ImportCommand, ImportRoots, InstallViewCommand, StoreCommand, StoreOutcome,
};

cipher_tests!(
    an_imported_heads_first_run_sends_its_imported_images,
    an_imported_heads_later_run_sends_its_new_image_beside_the_imported_ones,
    an_installed_view_over_the_imported_images_leaves_them_out_without_a_refusal,
);

/// A one-pixel PNG with an alpha channel: the image a tool returned in the imported history.
const TOOL_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==";

fn tool_png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(TOOL_PNG)
        .unwrap()
}

/// The image the imported prompt carries.
fn prompt_png() -> Vec<u8> {
    bytes_of(WIDE)
}

/// An OpenCode 2 export: a prompt with an image, a reply that takes a screenshot whose result is
/// an image, a reply, then a plain exchange. With `images` false, the same history in text only.
fn export(images: bool) -> Vec<u8> {
    let files = if images {
        json!([{"mime": "image/png", "data": base64(&prompt_png())}])
    } else {
        json!([])
    };
    let mut result = vec![json!({"type": "text", "text": "the screen"})];
    if images {
        result.push(json!({
            "type": "file",
            "mime": "image/png",
            "uri": format!("data:image/png;base64,{}", base64(&tool_png())),
        }));
    }
    serde_json::to_vec(&json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look at this.",
             "files": files},
            {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
             "content": [
                {"type": "text", "text": "Taking a screenshot."},
                {"type": "tool", "id": "call_1", "name": "screenshot",
                 "state": {"status": "completed", "input": {}, "content": result}}
             ]},
            {"id": "msg_a2", "type": "assistant", "time": {"created": 4000, "completed": 4500},
             "content": [{"type": "text", "text": "Both seen."}]},
            {"id": "msg_u2", "type": "user", "time": {"created": 5000}, "text": "Thanks."},
            {"id": "msg_a3", "type": "assistant", "time": {"created": 6000, "completed": 6500},
             "content": [{"type": "text", "text": "Any time."}]}
        ]
    }))
    .unwrap()
}

/// The import of `export` into the session `key`, owned by core under its head scope, its source
/// written under `dir`.
fn import_command(dir: &std::path::Path, key: &BindIdentity, export: &[u8]) -> StoreCommand {
    let folder = dir.join("imports").join(&key.session);
    std::fs::create_dir_all(&folder).unwrap();
    let path = folder.join("head.json");
    std::fs::write(&path, export).unwrap();
    let params = serde_json::from_value(json!({
        "import_id": "imp-1",
        "source": {
            "harness": "opencode2",
            "path": path.display().to_string(),
            "session": "ses_head",
            "sha256": digest(export),
        },
        "origin": {"kind": "import", "source_ref": "thalamus:ses_head"},
    }))
    .unwrap();
    let scope = match super::super::head_scope(super::super::core()) {
        broca_protocol::SendScope::Stamped(stamped) => stamped.scope,
        _ => unreachable!("a stamped scope"),
    };
    StoreCommand::Import(Box::new(ImportCommand {
        params,
        principal: super::super::core(),
        scope: Some(scope),
        roots: Arc::new(ImportRoots::new(vec![folder.clone()], Some(folder))),
        now_ms: 9_000,
    }))
}

/// Imports the history with images into `side`'s session; the id of the tool results message
/// the import split off the screenshot's reply.
async fn import(side: &Rendered) -> String {
    let command = import_command(&side.side.dir, &side.side.key, &export(true));
    let outcome = side
        .side
        .registry
        .route_store_command(&side.side.key, command)
        .await
        .unwrap();
    let StoreOutcome::Imported(reply) = outcome else {
        panic!("not imported: {outcome:?}")
    };
    side.side.caller.joins_existing();
    reply
        .moves
        .rows
        .iter()
        .find(|row| row.added)
        .expect("the screenshot's result is a message of its own")
        .mid
        .clone()
}

/// A store side with a compaction provider, its session imported, then its owner's first send
/// under a compacting plan, run to its end: the side and the plan.
async fn imported_side(
    steps: Vec<Vec<broca_protocol::NormalizedEvent>>,
    cipher: Cipher,
    view: Option<fn(&str) -> View>,
) -> (Rendered, Arc<broca_wal::PlanManifest>) {
    let compactor = Arc::new(super::super::controls::Compactor::default());
    let side = Rendered::with_compactor(true, steps, false, Some(compactor), cipher).await;
    let results_mid = import(&side).await;
    if let Some(view) = view {
        let installed = side
            .side
            .registry
            .route_store_command(
                &side.side.key,
                StoreCommand::InstallView(Box::new(InstallViewCommand {
                    view: serde_json::value::to_raw_value(&view(&results_mid)).unwrap(),
                    principal: super::super::core(),
                })),
            )
            .await
            .unwrap();
        assert!(
            matches!(installed, StoreOutcome::ViewInstalled(_)),
            "{installed:?}"
        );
    }
    let plan = super::super::controls::compacting_plan();
    let sent = send_planned(&side, "r1", planned(text("What did you see?")), &plan).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    side.side.idle().await;
    (side, plan)
}

/// The request at `index` the side's model was asked to send: its body, or the refusal that kept
/// it from being sent.
fn request_at(side: &Rendered, index: usize) -> String {
    let sent = side.sent();
    let body = sent
        .get(index)
        .unwrap_or_else(|| panic!("request {index} of {sent:?}"))
        .as_ref()
        .unwrap_or_else(|refusal| panic!("request {index} was refused: {refusal}"));
    String::from_utf8_lossy(body).into_owned()
}

fn position(body: &str, what: &str, name: &str) -> usize {
    body.find(what)
        .unwrap_or_else(|| panic!("{name} is not in the request: {body}"))
}

/// The first run of an imported head is sent, and carries both imported images' bytes where
/// their messages are: the prompt's image before the reply that took the screenshot, and the
/// screenshot after it, before the reply that follows.
async fn an_imported_heads_first_run_sends_its_imported_images(cipher: Cipher) {
    let (side, _) = imported_side(vec![text_step("a1", "I saw both.")], cipher, None).await;
    let body = request_at(&side, 0);
    let prompt = position(&body, &base64(&prompt_png()), "the prompt's image");
    let reply = position(&body, "Taking a screenshot.", "the screenshot's reply");
    let screenshot = position(&body, &base64(&tool_png()), "the screenshot");
    let after = position(&body, "Both seen.", "the reply after the screenshot");
    assert!(
        prompt < reply && reply < screenshot && screenshot < after,
        "the images are out of place: {body}"
    );
}

/// The owner's next run, with a new image, carries all three: both imported, and its own.
async fn an_imported_heads_later_run_sends_its_new_image_beside_the_imported_ones(cipher: Cipher) {
    let (side, plan) = imported_side(
        vec![
            text_step("a1", "I saw both."),
            text_step("a2", "And this one."),
        ],
        cipher,
        None,
    )
    .await;
    let upload = side.side.uploaded(&bytes_of(PNG)).await;
    let sent = send_planned(&side, "r2", planned(photo("look", &[&upload])), &plan).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    side.side.idle().await;
    assert_eq!(side.sent().len(), 2, "one request per run");
    let body = request_at(&side, 1);
    for (image, name) in [
        (prompt_png(), "the prompt's image"),
        (tool_png(), "the screenshot"),
        (bytes_of(PNG), "the new image"),
    ] {
        position(&body, &base64(&image), name);
    }
}

/// A view the head's compaction provider already applied, installed before the first run, that
/// leaves out every message through the screenshot's result: the first request holds neither
/// image, and is sent.
async fn an_installed_view_over_the_imported_images_leaves_them_out_without_a_refusal(
    cipher: Cipher,
) {
    fn cut_through(results_mid: &str) -> View {
        View::seal(
            1,
            vec![Edit::new(
                1,
                EditOp::Cut {
                    through: results_mid.into(),
                },
            )],
        )
    }
    let (side, _) = imported_side(
        vec![text_step("a1", "Nothing to see.")],
        cipher,
        Some(cut_through as fn(&str) -> View),
    )
    .await;
    let body = request_at(&side, 0);
    position(&body, "Thanks.", "the history the view keeps");
    for (image, name) in [
        (prompt_png(), "the prompt's image"),
        (tool_png(), "the screenshot"),
    ] {
        assert!(
            !body.contains(&base64(&image)),
            "{name} is in the request though the view leaves it out: {body}"
        );
    }
}

/// The session's records right after an import of `export`, through a store actor over a
/// session in memory.
async fn imported_records(
    export: &[u8],
) -> (
    BindIdentity,
    Vec<(u64, broca_session_store::record::Record)>,
    u64,
) {
    use broca_session_loop::parts::{MemoryParts, SharedParts};
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::session::Session;
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let key = BindIdentity::new("/project", "store-actor", "s1");
    let session = Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        broca_session_store::head::SessionTriple {
            project_root: "/project".into(),
            harness: "store-actor".into(),
            session: "s1".into(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let actor = crate::store_actor::StoreActor::new(
        key.clone(),
        session,
        Arc::new(crate::store_actor::tests::executor(Vec::new(), None)),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    );
    let (commands, receiver) = tokio::sync::mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let dir = broca_test_support::scratch_root().join(format!(
        "store-import-media-fold-{}-{}",
        std::process::id(),
        digest(export)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Store {
            command: import_command(&dir, &key, export),
            reply,
        })
        .unwrap();
    let outcome = answer.await.unwrap();
    assert!(matches!(outcome, StoreOutcome::Imported(_)), "{outcome:?}");
    drop(commands);
    let session = served.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let records = session.open_log_records().await.unwrap();
    (key, records, session.next_seq())
}

/// The media fold takes in an import's batch whole, with or without images in its history, and
/// stays able to render.
#[tokio::test]
async fn the_media_fold_takes_in_an_import_with_or_without_images() {
    for images in [true, false] {
        let (key, records, next) = imported_records(&export(images)).await;
        let mut fold = crate::store_media::MediaFold::new(key);
        fold.take(&records, &broca_session_loop::parts::MemoryParts::default());
        assert_eq!(fold.broken(), None, "images: {images}");
        assert_eq!(fold.next_seq(), next, "images: {images}");
    }
}
