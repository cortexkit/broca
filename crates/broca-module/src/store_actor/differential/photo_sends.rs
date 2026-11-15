//! Photo sends to a session kept in the new storage, through the module's registry, against the
//! same sends to a session on today's storage: a send that starts a run, a queued send, a steer
//! and a retracted queued send.
//!
//! An upload belongs to a session, so each session is created by a text send first. Then each
//! side uploads the same image and sends it. Every reply must be
//! equal, and so must `session.media_read` of the image (whole, by byte range, past its end, as a
//! metadata probe) and the media each message of the session's history holds. On the new
//! storage, the image's bytes are the session's own object: stored once, under the record that
//! first named them, and read back byte-equal after a restart.

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
/// first model step. A store side's session is in mode `cipher`.
async fn started(store: bool, gated: bool, cipher: Cipher) -> Side {
    let side = side_gated(store, steps(), gated, cipher).await;
    let mut first = request("first");
    scoped_by_core(&mut first);
    let sent = side.send("r1", first).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    if !gated {
        side.idle().await;
    }
    if store {
        assert_eq!(side.stored_mode(), cipher);
    }
    side
}

/// Today's side and the store's, each started alike, the store's session in mode `cipher`.
async fn both(gated: bool, cipher: Cipher) -> (Side, Side) {
    (
        started(false, gated, cipher).await,
        started(true, gated, cipher).await,
    )
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

/// Every media read of `image`, and of an image neither session's records name, answers alike.
async fn reads_alike(today: &Side, store: &Side, image: &[u8], when: &str) {
    let mut reads = reads_of(&digest(image), image.len() as u64);
    reads.push(json!({"sha256": digest(&bytes_of(WIDE)), "offset": 0, "length": 10}));
    for params in reads {
        alike!(
            store.media_read(params.clone()).await,
            today.media_read(params.clone()).await,
            "{when}: media_read {params}"
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
    let restarted = registry_at(&store.dir, true, Vec::new(), store.cipher).await;
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

/// Uploads `image` on each side and sends it as `delivery`, as run `run_id`: both replies.
async fn photo_send(
    today: &Side,
    store: &Side,
    run_id: &str,
    image: &[u8],
    delivery: broca_protocol::Delivery,
) -> (SendOutcome, SendOutcome) {
    let (old_id, new_id) = (today.uploaded(image).await, store.uploaded(image).await);
    (
        today
            .deliver(run_id, photo("look", &[&old_id]), delivery)
            .await,
        store
            .deliver(run_id, photo("look", &[&new_id]), delivery)
            .await,
    )
}

/// A photo send can't create its session with its photo: an upload needs a session to belong to,
/// so a creating send naming one is refused alike on both storages, and each keeps the session
/// the send opened, empty. The new storage is where such a session now starts.
async fn a_photo_send_creating_its_session_answers_alike(cipher: Cipher) {
    let (today, store) = (
        side_gated(false, steps(), false, cipher).await,
        side_gated(true, steps(), false, cipher).await,
    );
    let (mut old_send, mut new_send) = (
        photo("look", &["no-such-upload"]),
        photo("look", &["no-such-upload"]),
    );
    scoped_by_core(&mut old_send);
    scoped_by_core(&mut new_send);
    let old = today.send("r1", old_send).await;
    assert!(!matches!(old, SendOutcome::Active { .. }), "{old:?}");
    alike!(
        store.send("r1", new_send).await,
        old,
        "a photo send creating its session"
    );
    // Each storage keeps the session the send opened, as an empty one.
    alike_today_first!(today.registry.wal_written(&today.key), Some(true));
    assert!(store.registry.store_holding(&store.key).unwrap().is_some());
}

/// A photo send that starts a run answers alike, and so does one naming an upload that doesn't
/// exist. The image reads back alike, sits in the same message of the history, and survives a
/// restart of the store side byte-equal.
async fn a_photo_send_starting_a_run_answers_alike(cipher: Cipher) {
    let (today, store) = both(false, cipher).await;
    let image = bytes_of(PNG);
    let unknown = photo("look", &["no-such-upload"]);
    alike!(
        store.send("r2", unknown.clone()).await,
        today.send("r2", unknown).await,
        "a send naming no upload"
    );
    let (old, new) = photo_send(
        &today,
        &store,
        "r3",
        &image,
        broca_protocol::Delivery::Queue,
    )
    .await;
    assert!(matches!(old, SendOutcome::Active { .. }), "{old:?}");
    alike!(new, old, "a photo send starting a run");
    today.idle().await;
    store.idle().await;
    reads_alike(&today, &store, &image, "after its run").await;
    alike!(history_media(&store).await, history_media(&today).await);
    assert_eq!(media_objects(&store), 1);
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}

/// A photo send while a run runs is queued alike, and its image reads alike while it waits. When
/// it drains, its run's message points at the bytes its queued record stored, so the store keeps
/// one object for them, read back byte-equal after a restart.
async fn a_queued_photo_send_answers_alike_and_its_run_points_at_its_bytes(cipher: Cipher) {
    let (today, store) = both(true, cipher).await;
    let image = bytes_of(PNG);
    let (old, new) = photo_send(
        &today,
        &store,
        "r2",
        &image,
        broca_protocol::Delivery::Queue,
    )
    .await;
    assert!(matches!(old, SendOutcome::Pending { .. }), "{old:?}");
    alike!(new, old, "a queued photo send");
    reads_alike(&today, &store, &image, "while it is queued").await;
    assert_eq!(media_objects(&store), 1, "stored with its queued record");
    today.release_to_idle().await;
    store.release_to_idle().await;
    reads_alike(&today, &store, &image, "after its run").await;
    alike!(history_media(&store).await, history_media(&today).await);
    assert_eq!(
        media_objects(&store),
        1,
        "its run points at the same object"
    );
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}

/// A photo steered into a running run is taken alike, and its image reads alike before and after
/// the boundary that places it. The placed copy points at the bytes the steer stored.
async fn a_steered_photo_answers_alike_and_its_copy_points_at_its_bytes(cipher: Cipher) {
    let (today, store) = both(true, cipher).await;
    let image = bytes_of(PNG);
    let (old, new) = photo_send(
        &today,
        &store,
        "r2",
        &image,
        broca_protocol::Delivery::Steer,
    )
    .await;
    assert!(matches!(old, SendOutcome::Active { .. }), "{old:?}");
    alike!(new, old, "a steered photo");
    reads_alike(&today, &store, &image, "before its boundary").await;
    today.release_to_idle().await;
    store.release_to_idle().await;
    reads_alike(&today, &store, &image, "after its boundary").await;
    let history = history_media(&store).await;
    alike!(history, history_media(&today).await);
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

/// A queued photo send retracted before it runs answers alike. Its image stays readable alike,
/// as today's session keeps every medium a record captured: on the new storage its bytes stay the
/// session's object, named by the queued record the retraction leaves in the history.
async fn a_retracted_photo_send_answers_alike_and_its_bytes_stay_readable(cipher: Cipher) {
    let (today, store) = both(true, cipher).await;
    let image = bytes_of(PNG);
    let (old, new) = photo_send(
        &today,
        &store,
        "r2",
        &image,
        broca_protocol::Delivery::Queue,
    )
    .await;
    alike!(new, old, "a queued photo send");
    let SendOutcome::Pending { submission_id } = old else {
        panic!("queued: {old:?}");
    };
    let old = today
        .registry
        .route_retract(&today.key, submission_id.clone())
        .await
        .unwrap();
    let new = store
        .registry
        .route_retract(&store.key, submission_id)
        .await
        .unwrap();
    alike!(new, old, "the retraction");
    reads_alike(&today, &store, &image, "once retracted").await;
    today.release_to_idle().await;
    store.release_to_idle().await;
    reads_alike(&today, &store, &image, "after the run").await;
    alike!(history_media(&store).await, history_media(&today).await);
    reads_back(&store, &image).await;
    survives_a_restart(&store, &image).await;
}
