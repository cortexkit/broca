//! Reads that run their own code over the session as stored (`session.tag_map`,
//! `session.tag_read`, `session.request_dump`) take the same path as every other read: an open
//! actor makes its records durable before the read sees them, and a closed session that needs
//! a recovery is recovered first.

use super::*;

/// Where the module's session stands as stored: its head version and its open log's length.
async fn position(m: &Module) -> broca_session_store::session::Position {
    Session::stored_position(&m.backend(), &m.addr(), &deployment_key())
        .await
        .unwrap()
        .expect("the session has a head")
}

/// A module whose run r1 is stopped at its first tool call's approval: after the step that made
/// the call was appended without a barrier, and before the batch's intents make it durable.
/// Returns the module and the approval's `release`.
async fn stopped_in_the_gap() -> (Module, Arc<Notify>) {
    let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let approval = (Arc::clone(&reached), Arc::clone(&release));
    let m = module_with(move |gate| {
        let mut executor = executor(vec![read_step("a1"), text_step("a2", "done")], Some(gate));
        executor.approval_gate = Some(approval);
        Arc::new(executor)
    })
    .await;
    active(m.send("r1", "first").await);
    m.gate.notify_one();
    reached.notified().await;
    (m, release)
}

/// Lets the stopped run finish, and stops the module.
async fn finish(m: Module, release: Arc<Notify>) {
    release.notify_one();
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    assert!(m.registry.stop_and_seal().await);
}

/// A `session.tag_map` read while the actor holds records appended without a barrier makes them
/// durable first: the log grows by the claim frame that carries the barrier. A second read finds
/// nothing left to make durable.
#[tokio::test]
async fn a_tag_map_read_makes_the_actors_pending_records_durable_first() {
    let (m, release) = stopped_in_the_gap().await;
    let runtime = m.runtime();
    let params = broca_wire::tag_map::TagMapParams::default();
    let before = position(&m).await;
    crate::store_tag_map::serve(&m.registry, &runtime, &m.key, &params)
        .await
        .expect("the tag map is served");
    let after = position(&m).await;
    assert_eq!(after.log, before.log);
    assert!(
        after.len > before.len,
        "the actor made its pending records durable before the read: {before:?} -> {after:?}"
    );
    crate::store_tag_map::serve(&m.registry, &runtime, &m.key, &params)
        .await
        .expect("the tag map is served again");
    assert_eq!(
        position(&m).await,
        after,
        "nothing was left to make durable"
    );
    finish(m, release).await;
}

/// A `session.request_dump` read while the actor holds records appended without a barrier makes
/// them durable first, as `session.tag_map` does.
#[tokio::test]
async fn a_request_dump_makes_the_actors_pending_records_durable_first() {
    let (m, release) = stopped_in_the_gap().await;
    let runtime = m.runtime();
    let before = position(&m).await;
    // The scripted provider records no request digest, so the run may have no step to rebuild;
    // the read of the session happens either way.
    match crate::store_request_dump::serve(&m.registry, &runtime, &m.key, "r1").await {
        Ok(_) | Err(crate::serve::ServeError::UnknownRun(_)) => {}
        Err(error) => panic!("the session is read: {error:?}"),
    }
    let after = position(&m).await;
    assert_eq!(after.log, before.log);
    assert!(
        after.len > before.len,
        "the actor made its pending records durable before the read: {before:?} -> {after:?}"
    );
    finish(m, release).await;
}

/// A closed session whose unfinished run a recovery would seal is recovered before
/// `session.tag_map` reads it, as every other read recovers it, rather than read as it stood
/// before the recovery.
#[tokio::test]
async fn a_tag_map_read_recovers_a_closed_session_first() {
    let m = module(vec![text_step("a1", "seed")]).await;
    active(m.send("r1", "seed").await);
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    m.close().await;
    // A run whose start is durable and nothing after it: what a crash mid-run leaves.
    let mut writer = m.writer().await;
    let mut start = writer
        .operations
        .run
        .as_ref()
        .unwrap()
        .start
        .clone()
        .unwrap();
    start.run_id = "crashed".into();
    start.submission_id = None;
    start.send_id = None;
    writer
        .append(&[Record::RunStart(Box::new(start))], true)
        .await
        .unwrap();
    writer.close().await.unwrap();
    let before = position(&m).await;
    crate::store_tag_map::serve(
        &m.registry,
        &m.runtime(),
        &m.key,
        &broca_wire::tag_map::TagMapParams::default(),
    )
    .await
    .expect("the tag map is served");
    assert_ne!(
        position(&m).await,
        before,
        "the crashed run was recovered before the read"
    );
    assert!(m.registry.stop_and_seal().await);
}

/// A `session.request_dump` whose sealed history can't be decoded is refused as permanent: the
/// damage reads the same on every try.
#[tokio::test]
async fn a_request_dump_over_damaged_sealed_history_is_refused_as_permanent() {
    let m = module(vec![text_step("a1", "one"), text_step("a2", "two")]).await;
    active(m.send("r1", "first").await);
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    active(m.send("r2", "second").await);
    m.gate.notify_one();
    m.ended("r2", "completed").await;
    m.close().await;
    // Seal at a compaction cut before r2's input, so r1's records move to a sealed segment.
    let mut writer = m.writer().await;
    let cut = input_of(&writer, "r2").await;
    let (ordinal, mid) = entry_of(&writer, "second").await;
    writer
        .append(
            &[Record::Marker(Marker {
                view_version: 0,
                first_kept_mid: mid,
                first_kept_ordinal: ordinal,
                checkpoint: 0,
                harness: Some(m.key.harness.clone()),
                session: Some(m.key.session.clone()),
            })],
            true,
        )
        .await
        .unwrap();
    writer.seal(Some(cut), 1).await.unwrap();
    writer.close().await.unwrap();
    // Flip the last byte of every sealed transcript segment, so none authenticates.
    let objects = m
        .dir
        .join(broca_records::SESSION_STORAGE_DIR)
        .join("objects")
        .join(&m.addr().0);
    let mut damaged = 0;
    let mut folders = vec![objects];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                folders.push(path);
            } else if path.file_name().is_some_and(|name| name == "t") {
                let mut bytes = std::fs::read(&path).unwrap();
                *bytes.last_mut().unwrap() ^= 0xff;
                std::fs::remove_file(&path).unwrap();
                std::fs::write(&path, bytes).unwrap();
                damaged += 1;
            }
        }
    }
    assert!(damaged > 0, "the seal wrote a transcript segment");
    match crate::store_request_dump::serve(&m.registry, &m.runtime(), &m.key, "r1").await {
        Err(crate::serve::ServeError::Open(_, class)) => {
            assert_eq!(class, Some(broca_protocol::ErrorClass::Permanent));
        }
        other => panic!("the dump is refused as damaged: {other:?}"),
    }
    assert!(m.registry.stop_and_seal().await);
}
