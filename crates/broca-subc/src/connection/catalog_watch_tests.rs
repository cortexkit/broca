use std::sync::Arc;
use std::time::Duration;

use subc_control::{CatalogEntry, CatalogWatchResponse, ClientControlResponse};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use super::super::tests::route_open_test_connection;
use super::super::{control_flags, PendingEntry, PendingMap, SubcConnection};

#[tokio::test(start_paused = true)]
async fn catalog_list_waits_for_the_replacement_during_daemon_upgrade() {
    let (conn, _, _) = SubcConnection::in_process_for_test(7, 1);
    let (replacement, _, mut daemon) = SubcConnection::in_process_for_test(8, 2);
    conn.deactivate_for_test();
    let ceiling = tokio::time::Instant::now() + Duration::from_secs(5);
    crate::daemon_upgrade::announce(ceiling);
    let read = conn.catalog_list();
    tokio::pin!(read);
    assert!(
        futures::poll!(&mut read).is_pending(),
        "catalog.list must wait for the replacement consumer before batch setup can fail"
    );
    crate::daemon_upgrade::resumed(ceiling);
    conn.replace_generation_for_test(&replacement);
    let request = tokio::select! {
        sent = daemon.next_request() => sent,
        done = &mut read => panic!("catalog read ended before sending on the replacement: {done:?}"),
    };
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["op"],
        "catalog.list"
    );
    let catalog = ClientControlResponse::CatalogList {
        generation: 1,
        modules: vec![CatalogEntry::new("replacement-tools", vec![], vec![])],
        subc_ops: vec![],
    };
    assert!(daemon.reply(&request, serde_json::to_vec(&catalog).unwrap()));
    let catalog = read.await.unwrap();
    assert_eq!(catalog[0].module_id, "replacement-tools");
    crate::daemon_upgrade::abandon();
}

/// The next frame the connection wrote, with its op.
async fn next_request(write_rx: &mut mpsc::Receiver<Frame>) -> (Frame, String) {
    let frame = tokio::time::timeout(Duration::from_secs(5), write_rx.recv())
        .await
        .expect("the connection wrote nothing")
        .unwrap();
    let op = serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["op"]
        .as_str()
        .unwrap()
        .to_owned();
    (frame, op)
}

fn answer(pending: &PendingMap, request: &Frame, ty: FrameType, flags: Flags, body: Vec<u8>) {
    let sender = match pending.lock().unwrap().get(&(0, 0, request.header.corr)) {
        Some(PendingEntry::Live { sender, .. }) => sender.clone(),
        _ => panic!("nothing waits for this request"),
    };
    let frame = Frame::build(ty, flags, 0, 0, request.header.corr, body).unwrap();
    sender.send(frame).unwrap();
}

fn push(pending: &PendingMap, watch: &Frame, generation: u64) {
    answer(
        pending,
        watch,
        FrameType::Response,
        Flags::new(false, Priority::Passive, false),
        serde_json::to_vec(&CatalogWatchResponse::new(generation)).unwrap(),
    );
}

fn catalog_reply(pending: &PendingMap, request: &Frame, generation: u64, module: &str) {
    answer(
        pending,
        request,
        FrameType::Response,
        control_flags(),
        serde_json::to_vec(&ClientControlResponse::CatalogList {
            generation,
            modules: vec![CatalogEntry::new(module, Vec::new(), Vec::new())],
            subc_ops: Vec::new(),
        })
        .unwrap(),
    );
}

/// `catalog_list` once, answering the read it sends with `generation` and `module`.
async fn listed_after_a_read(
    conn: &SubcConnection,
    write_rx: &mut mpsc::Receiver<Frame>,
    pending: &PendingMap,
    generation: u64,
    module: &str,
) -> Vec<String> {
    let list = conn.catalog_list();
    tokio::pin!(list);
    // A watch the follower opens again after one ended may be written first; it isn't a read.
    let request = loop {
        let request = tokio::select! {
            listed = &mut list => panic!("served without a read: {listed:?}"),
            request = next_request(write_rx) => request,
        };
        if request.1 != "catalog.watch" {
            break request;
        }
    };
    assert_eq!(request.1, "catalog.list");
    catalog_reply(pending, &request.0, generation, module);
    names(list.await.unwrap())
}

/// `catalog_list` once, which must not send anything.
async fn listed_without_a_read(
    conn: &SubcConnection,
    write_rx: &mut mpsc::Receiver<Frame>,
) -> Vec<String> {
    let listed = conn.catalog_list().await.unwrap();
    assert!(
        write_rx.try_recv().is_err(),
        "a held catalog was read again"
    );
    names(listed)
}

fn names(entries: Arc<Vec<CatalogEntry>>) -> Vec<String> {
    entries
        .iter()
        .map(|entry| entry.module_id.clone())
        .collect()
}

/// Wait until the watch task has recorded `generation`.
async fn recorded(conn: &SubcConnection, generation: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while conn
            .shared
            .catalog
            .state
            .lock()
            .unwrap()
            .pushed
            .map(|(_, g)| g)
            != Some(generation)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the push was never recorded");
}

#[tokio::test]
async fn a_followed_catalog_is_read_again_only_when_its_generation_moves() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let conn = Arc::new(conn);
    conn.follow_catalog();
    let (watch, op) = next_request(&mut write_rx).await;
    assert_eq!(op, "catalog.watch");
    assert!(watch.header.flags.is_subscription());

    // Before the first push nothing is known to be current.
    let first = listed_after_a_read(&conn, &mut write_rx, &pending, 3, "aft").await;
    assert_eq!(first, ["aft"]);

    push(&pending, &watch, 3);
    recorded(&conn, 3).await;
    // The reply above was read before the push arrived, at the same generation.
    assert_eq!(listed_without_a_read(&conn, &mut write_rx).await, ["aft"]);
    assert_eq!(listed_without_a_read(&conn, &mut write_rx).await, ["aft"]);

    push(&pending, &watch, 5);
    recorded(&conn, 5).await;
    let moved = listed_after_a_read(&conn, &mut write_rx, &pending, 5, "magic-context").await;
    assert_eq!(moved, ["magic-context"]);
    assert_eq!(
        listed_without_a_read(&conn, &mut write_rx).await,
        ["magic-context"]
    );
}

#[tokio::test]
async fn a_refused_watch_leaves_every_list_a_read() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let conn = Arc::new(conn);
    conn.follow_catalog();
    let (watch, _) = next_request(&mut write_rx).await;
    push(&pending, &watch, 2);
    recorded(&conn, 2).await;
    assert_eq!(
        listed_after_a_read(&conn, &mut write_rx, &pending, 2, "aft").await,
        ["aft"]
    );
    answer(
        &pending,
        &watch,
        FrameType::Error,
        control_flags(),
        serde_json::to_vec(&ErrorBody::new("unsupported_op", "no catalog.watch")).unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while conn.shared.catalog.state.lock().unwrap().pushed.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the ended watch still vouches for the catalog");
    assert_eq!(
        listed_after_a_read(&conn, &mut write_rx, &pending, 2, "aft").await,
        ["aft"]
    );
}

#[tokio::test]
async fn an_unfollowed_connection_reads_every_time() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    for _ in 0..2 {
        assert_eq!(
            listed_after_a_read(&conn, &mut write_rx, &pending, 1, "aft").await,
            ["aft"]
        );
    }
}

/// A watch stays registered for the connection's whole life, so it must not count as work the
/// daemon is busy with: if it did, a liveness probe would wait for it forever and a socket that
/// went silent would never be detected or replaced.
#[tokio::test]
async fn an_open_watch_never_holds_back_a_liveness_probe() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let conn = Arc::new(conn);
    conn.follow_catalog();
    let (watch, op) = next_request(&mut write_rx).await;
    assert_eq!(op, "catalog.watch");
    push(&pending, &watch, 3);
    recorded(&conn, 3).await;
    let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
    assert!(
        super::super::channel_zero_probe_blocker(&runtime, std::time::Instant::now()).is_none(),
        "the open watch held back the liveness probe"
    );
}

/// A held catalog is shared with every caller it serves, not copied for each.
#[tokio::test]
async fn a_held_catalog_is_served_without_a_copy() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let conn = Arc::new(conn);
    conn.follow_catalog();
    let (watch, _) = next_request(&mut write_rx).await;
    push(&pending, &watch, 3);
    recorded(&conn, 3).await;
    assert_eq!(
        listed_after_a_read(&conn, &mut write_rx, &pending, 3, "aft").await,
        ["aft"]
    );
    let first = conn.catalog_list().await.unwrap();
    let second = conn.catalog_list().await.unwrap();
    assert!(
        write_rx.try_recv().is_err(),
        "a held catalog was read again"
    );
    assert!(Arc::ptr_eq(&first, &second));
}

/// Following the catalog never keeps a connection alive: once its last owner drops it, the
/// connection closes and its watch ends, though the daemon never ended the feed.
#[tokio::test]
async fn dropping_the_last_owner_of_a_followed_connection_ends_its_watch() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let conn = Arc::new(conn);
    conn.follow_catalog();
    let (watch, _) = next_request(&mut write_rx).await;
    push(&pending, &watch, 3);
    recorded(&conn, 3).await;
    let followed = Arc::downgrade(&conn);
    drop(conn);
    tokio::time::timeout(Duration::from_secs(5), async {
        while followed.strong_count() > 0 || !pending.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the watch kept the connection alive after its last owner dropped it");
}
