use super::*;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::SessionAddr;
use broca_session_store::codec::hex;
use broca_session_store::record::Moved;

#[tokio::test]
async fn the_pinned_held_call_read_classifies_like_a_session_view() {
    let rig = Rig::new("held-intake-pinned-equality", None, vec![]).await;
    let class = StorageClass::Ordinary;
    let backend = rig.runtime.class_backend(class);
    let deployment = rig.runtime.deployment();

    // The held run and its intent are already sealed. Its key needs the sealed id index, and
    // the checkpoint carries the latest run. A missing converted key is not foreign either.
    let sealed = session("sealed-held-intake");
    let call_key = "pre-conversion-log:4";
    rig.seed(&sealed, &held_records(call_key, "tools")).await;
    let mut writer = Session::load(
        &backend,
        SessionAddr(broca_core::session_addr(&sealed)),
        &crate::store_open::triple(&sealed),
        deployment,
    )
    .await
    .unwrap();
    writer
        .append(
            &[Record::Moved(Moved {
                move_id: "store-move:fixture".into(),
                source_activity_ms: 43,
                source_lineage: Some("converted".into()),
            })],
            true,
        )
        .await
        .unwrap();
    writer.seal(None, 0).await.unwrap();
    assert!(writer.head().id_index.is_some());
    assert!(writer
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .all(|(_, record)| !matches!(record, Record::ToolIntent(_) | Record::RunStart(_))));
    writer.close().await.unwrap();
    assert_same_classification(
        &rig,
        &sealed,
        &[
            (call_key, Classified::Delivered, None),
            (
                "converted:5",
                Classified::Blocked("intent_missing".into()),
                None,
            ),
        ],
    )
    .await;

    let replaced = session("replaced-held-intake");
    let addr = SessionAddr(broca_core::session_addr(&replaced));
    let mut writer = Session::create(
        &backend,
        addr.clone(),
        crate::store_open::triple(&replaced),
        deployment,
        false,
    )
    .await
    .unwrap();
    let old_key = format!("{}:4", hex(&writer.head().session_id.0));
    writer
        .append(&held_records(&old_key, "tools"), true)
        .await
        .unwrap();
    let version = writer.position().version;
    writer.close().await.unwrap();
    Session::delete(&backend, &addr, version).await.unwrap();
    assert!(
        StoredLineage::read(&rig.runtime, None, &replaced, vec![old_key.clone()])
            .await
            .is_none()
    );
    rig.seed(&replaced, &[run_start("replacement")]).await;
    assert_same_classification(
        &rig,
        &replaced,
        &[(&old_key, Classified::NoOp, Some("session_deleted"))],
    )
    .await;

    let damaged = session("damaged-held-intake");
    rig.seed(&damaged, &held_records(call_key, "tools")).await;
    let addr = SessionAddr(broca_core::session_addr(&damaged));
    let position = Session::stored_position(&backend, &addr, deployment)
        .await
        .unwrap()
        .unwrap();
    let log = class
        .store_root(&rig.root)
        .join("logs")
        .join(addr.0)
        .join(hex(&position.log.0));
    std::fs::write(log, [0]).unwrap();
    assert_same_classification(
        &rig,
        &damaged,
        &[(
            call_key,
            Classified::Blocked("session_store_unreadable".into()),
            None,
        )],
    )
    .await;
}

async fn assert_same_classification(
    rig: &Rig,
    bind: &BindIdentity,
    cases: &[(&str, Classified, Option<&str>)],
) {
    let keys: Vec<_> = cases.iter().map(|(key, _, _)| (*key).to_owned()).collect();
    let baseline = match rig.runtime.current_view_for(bind, "comparison").await {
        Ok(Some(view)) => held_call_records(&view, &keys)
            .await
            .map_or(StoredLineage::Unreadable, StoredLineage::from),
        Err(_) => StoredLineage::Unreadable,
        Ok(None) => panic!("expected a stored session for {bind:?}"),
    };
    let pinned = StoredLineage::read(&rig.runtime, None, bind, keys)
        .await
        .unwrap();
    for (key, expected, reason) in cases {
        let entry = entry(kinds::RESULT, key, "event");
        let row = RegistrationRow {
            call: registration(key, "tools", bind),
            state: RegistrationState::Registered,
        };
        assert_eq!(&baseline.classify("tools", &entry, &row), expected, "{key}");
        assert_eq!(&pinned.classify("tools", &entry, &row), expected, "{key}");
        assert_eq!(baseline.retirement_reason(key), *reason, "{key}");
        assert_eq!(pinned.retirement_reason(key), *reason, "{key}");
        assert_eq!(
            baseline.needs_intake(key),
            pinned.needs_intake(key),
            "{key}"
        );
        assert_eq!(
            baseline.withdrawal_never_owed(key),
            pinned.withdrawal_never_owed(key),
            "{key}"
        );
        assert_eq!(
            baseline.held_call_id(key),
            pinned.held_call_id(key),
            "{key}"
        );
    }
}
