//! Photo sends to a session kept in the session store, through the module's registry: a send that
//! starts a run, a queued send, a steer and a retracted queued send.
//!
//! An upload belongs to a session, so the session is created by a text send first. Then the image
//! is uploaded and sent. Every reply is pinned, and so are `session.media_read` of the image
//! (whole, by byte range, past its end, as a metadata probe) and the media each message of the
//! session's history holds. The image's bytes are the session's own object: stored once, under
//! the record that first named them, and read back byte-equal after a restart.

use broca_session_store::keys::Cipher;
use serde_json::json;

use super::media::{
    base64, bytes_of, digest, photo, reads_of, registry_at, side_gated, Side, PNG, WIDE,
};
use super::{request, scoped_by_core};
use crate::actor::SendOutcome;
use crate::serve::ServeHandler;

cipher_tests!(
    a_photo_send_creating_its_session_answers_alike,
    a_photo_send_starting_a_run_answers_alike,
    a_queued_photo_send_answers_alike_and_its_run_points_at_its_bytes,
    a_steered_photo_answers_alike_and_its_copy_points_at_its_bytes,
    a_retracted_photo_send_answers_alike_and_its_bytes_stay_readable,
);

fn steps() -> Vec<Vec<broca_protocol::NormalizedEvent>> {
    vec![
        super::super::tests::text_step("a1", "hello"),
        super::super::tests::text_step("a2", "seen"),
        super::super::tests::text_step("a3", "noted"),
    ]
}

/// A side whose session exists: core's first text send ran, and, when `gated`, is still in its
/// first model step. The session is in mode `cipher`.
async fn started(gated: bool, cipher: Cipher) -> Side {
    let side = side_gated(steps(), gated, cipher).await;
    let mut first = request("first");
    scoped_by_core(&mut first);
    let sent = side.send("r1", first).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    if !gated {
        side.idle().await;
    }
    assert_eq!(side.stored_mode(), cipher);
    side
}

/// The role and the digests of the media each message of the side's history holds, in order, as
/// `session.read` serves them.
async fn history_media(side: &Side) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    let mut from = Some(1);
    loop {
        let request = crate::serve::SessionReadRequest::new(side.key.clone(), from, None);
        let page = ServeHandler::read(&side.registry, request)
            .await
            .expect("the session reads");
        for entry in &page.messages {
            let media =
                broca_provider::person_media::references(std::slice::from_ref(&entry.message))
                    .map(|media| media.sha256.clone())
                    .collect();
            out.push((format!("{:?}", entry.message.role), media));
        }
        match page.next_from_ordinal {
            Some(next) if !page.messages.is_empty() => from = Some(next),
            _ => return out,
        }
    }
}

/// Pins every media read of `image`, and of an image the session's records don't name.
async fn reads_alike(store: &Side, image: &[u8], when: &str) {
    let mut reads = reads_of(&digest(image), image.len() as u64);
    reads.push(json!({"sha256": digest(&bytes_of(WIDE)), "offset": 0, "length": 10}));
    for params in reads {
        broca_test_support::pin!(
            format!("{when}: media_read {params}"),
            store.media_read(params.clone()).await
        );
    }
}

/// The store side's image, read whole: byte-equal to `image`.
async fn reads_back(store: &Side, image: &[u8]) {
    let whole = store
        .media_read(json!({"sha256": digest(image), "offset": 0, "length": image.len()}))
        .await
        .unwrap();
    assert_eq!(whole["data"], json!(base64(image)));
}

/// How many media objects the store side's sessions keep on disk.
fn media_objects(store: &Side) -> usize {
    fn count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|entry| {
                        let path = entry.path();
                        if path.is_dir() {
                            count(&path)
                        } else {
                            usize::from(entry.file_name().to_string_lossy().contains("-media-"))
                        }
                    })
                    .sum()
            })
            .unwrap_or(0)
    }
    count(&store.dir.join(broca_wal::SESSION_STORAGE_DIR))
}

/// The store side, restarted on its state root: its image reads back byte-equal, whole and by
/// byte range.
async fn survives_a_restart(store: &Side, image: &[u8]) {
    assert!(store.registry.stop_and_seal().await);
    let restarted = registry_at(&store.dir, Vec::new(), store.cipher).await;
    let whole = ServeHandler::media_read(
        &restarted,
        &store.key,
        json!({"sha256": digest(image), "offset": 0, "length": image.len()}),
    )
    .await
    .unwrap();
    assert_eq!(whole["data"], json!(base64(image)), "after a restart");
    let range = ServeHandler::media_read(
        &restarted,
        &store.key,
        json!({"sha256": digest(image), "offset": 7, "length": 9}),
    )
    .await
    .unwrap();
    assert_eq!(
        range["data"],
        json!(base64(&image[7..16])),
        "after a restart"
    );
    assert!(restarted.stop_and_seal().await);
}

/// Uploads `image` and sends it as `delivery`, as run `run_id`: the reply.
async fn photo_send(
    store: &Side,
    run_id: &str,
    image: &[u8],
    delivery: broca_protocol::Delivery,
) -> SendOutcome {
    let id = store.uploaded(image).await;
    store.deliver(run_id, photo("look", &[&id]), delivery).await
}

/// A photo send can't create its session with its photo: an upload needs a session to belong to,
/// so a creating send naming one is refused, and the session store keeps the session the send
/// opened, empty.
async fn a_photo_send_creating_its_session_answers_alike(cipher: Cipher) {
    let store = side_gated(steps(), false, cipher).await;
    let mut send = photo("look", &["no-such-upload"]);
    scoped_by_core(&mut send);
    let new = store.send("r1", send).await;
    assert!(!matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("a photo send creating its session", new);
    // The storage keeps the session the send opened, as an empty one.
    assert!(store.registry.store_holding(&store.key).unwrap().is_some());
}

/// A photo send that starts a run, and one naming an upload that doesn't exist. The image reads
/// back, sits in its message of the history, and survives a restart byte-equal.
async fn a_photo_send_starting_a_run_answers_alike(cipher: Cipher) {
    let store = started(false, cipher).await;
    let image = bytes_of(PNG);
    let unknown = photo("look", &["no-such-upload"]);
    broca_test_support::pin!("a send naming no upload", store.send("r2", unknown).await);
    let new = photo_send(&store, "r3", &image, broca_protocol::Delivery::Queue).await;
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("a photo send starting a run", new);
    store.idle().await;
    reads_alike(&store, &image, "after its run").await;
    broca_test_support::pin!("alike", history_media(&store).await);
    assert_eq!(media_objects(&store), 1);
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}

/// A photo send while a run runs is queued, and its image reads while it waits. When it drains,
/// its run's message points at the bytes its queued record stored, so the store keeps one object
/// for them, read back byte-equal after a restart.
async fn a_queued_photo_send_answers_alike_and_its_run_points_at_its_bytes(cipher: Cipher) {
    let store = started(true, cipher).await;
    let image = bytes_of(PNG);
    let new = photo_send(&store, "r2", &image, broca_protocol::Delivery::Queue).await;
    assert!(matches!(new, SendOutcome::Pending { .. }), "{new:?}");
    broca_test_support::pin!("a queued photo send", new);
    reads_alike(&store, &image, "while it is queued").await;
    assert_eq!(media_objects(&store), 1, "stored with its queued record");
    store.release_to_idle().await;
    reads_alike(&store, &image, "after its run").await;
    broca_test_support::pin!("alike", history_media(&store).await);
    assert_eq!(
        media_objects(&store),
        1,
        "its run points at the same object"
    );
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}

/// A photo steered into a running run is taken, and its image reads before and after the
/// boundary that places it. The placed copy points at the bytes the steer stored.
async fn a_steered_photo_answers_alike_and_its_copy_points_at_its_bytes(cipher: Cipher) {
    let store = started(true, cipher).await;
    let image = bytes_of(PNG);
    let new = photo_send(&store, "r2", &image, broca_protocol::Delivery::Steer).await;
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("a steered photo", new);
    reads_alike(&store, &image, "before its boundary").await;
    store.release_to_idle().await;
    reads_alike(&store, &image, "after its boundary").await;
    let history = history_media(&store).await;
    broca_test_support::pin!("alike", history);
    assert!(
        history.iter().any(|(_, media)| media == &[digest(&image)]),
        "the steer's copy holds the image: {history:?}"
    );
    assert_eq!(
        media_objects(&store),
        1,
        "the copy points at the steer's object"
    );
    survives_a_restart(&store, &image).await;
}

/// A queued photo send retracted before it runs. Its image stays readable: its bytes stay the
/// session's object, named by the queued record the retraction leaves in the history.
async fn a_retracted_photo_send_answers_alike_and_its_bytes_stay_readable(cipher: Cipher) {
    let store = started(true, cipher).await;
    let image = bytes_of(PNG);
    let new = photo_send(&store, "r2", &image, broca_protocol::Delivery::Queue).await;
    broca_test_support::pin!("a queued photo send", new);
    let SendOutcome::Pending { submission_id } = new else {
        panic!("queued: {new:?}");
    };
    let new = store
        .registry
        .route_retract(&store.key, submission_id)
        .await
        .unwrap();
    broca_test_support::pin!("the retraction", new);
    reads_alike(&store, &image, "once retracted").await;
    store.release_to_idle().await;
    reads_alike(&store, &image, "after the run").await;
    broca_test_support::pin!("alike", history_media(&store).await);
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}
