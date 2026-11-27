use super::*;
use crate::serve::ServeHandler;
use broca_protocol::RunId;

#[tokio::test]
async fn closed_run_status_wait_matches_fresh_bytes_before_and_after_deadline() {
    for (name, resume_ms, next_ms) in [
        (
            "status-wait-future",
            PAUSED_RESUME_MS,
            Some(PAUSED_RESUME_MS),
        ),
        ("status-wait-past", 1_600_000_000_000, None),
    ] {
        let (root, runtime, registry, key) =
            paused_closed_read_fixture_until(name, resume_ms).await;
        let class = StorageClass::Ordinary;
        let run = RunId("paused".into());
        let stored = Session::view(
            runtime.backend(class),
            SessionAddr(broca_core::session_addr(&key)),
            &triple(&key),
            runtime.deployment(),
        )
        .await
        .unwrap();
        let fresh = crate::store_status::Reader {
            labels: Default::default(),
            parts: runtime.parts_for(class, stored.head().cipher()),
            live: false,
            settled: None,
            session_name: &key.session,
            live_step: None,
        }
        .run_status_reply(&stored, &run)
        .await
        .unwrap();
        let fresh_bytes = serde_json::to_vec(&fresh).unwrap();
        let actual_json = serde_json::to_value(fresh).unwrap();
        assert_eq!(actual_json["state"], "paused");
        assert_eq!(actual_json["resume_not_before_ms"], resume_ms);
        assert_eq!(actual_json["awaits"]["next_ms"].as_u64(), next_ms);
        for _ in 0..2 {
            let actual = registry.run_status_reply(&key, run.clone()).await.unwrap();
            assert_eq!(serde_json::to_vec(&actual).unwrap(), fresh_bytes, "{name}");
        }
        assert!(registry.slot_state(&key).is_none());
        drop(registry);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }
}
