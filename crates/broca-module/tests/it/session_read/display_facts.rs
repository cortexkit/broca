use super::*;

#[tokio::test]
async fn display_fact_backfill_is_lazy_persistent_and_reads_are_bounded_at_1k_and_150k() {
    for size in [1_000, 150_000] {
        let fixture_started = std::time::Instant::now();
        let (rig, zero_ranges) = Rig::with_counting_zero_ranges("display-fact-cost");
        let key = rig.key("facts");
        let mut records = vec![started(&key, "prefix", "start")];
        records.extend((0..size - 5).map(|step| assistant_text(step, "history")));
        records.push(finished_ok());
        let mut tool = dispatch_module_episode(&key, "tools", "fact-call");
        for record in &mut tool {
            match record {
                WalRecord::ToolDispatchIntent {
                    dispatched_at_ms, ..
                } => *dispatched_at_ms = Some(100),
                WalRecord::ToolResult { finished_at_ms, .. } => *finished_at_ms = Some(150),
                _ => {}
            }
        }
        records.extend(tool);
        let records_ms = fixture_started.elapsed().as_secs_f64() * 1000.0;
        let append_started = std::time::Instant::now();
        // This setup does not test sync behavior. Batch syncs so fixture writes do
        // not dominate the page-read measurements on Windows filesystems.
        {
            let mut wal = FileWal::open(rig.wal_store.wal_path(&key))
                .expect("open display-facts fixture WAL")
                .with_feature_floor(rig.wal_store.feature_floor_path());
            for batch in records.chunks(1_000) {
                for (index, record) in batch.iter().enumerate() {
                    wal.append(1, record, index + 1 == batch.len())
                        .expect("append fixture record");
                }
            }
        }
        let append_ms = append_started.elapsed().as_secs_f64() * 1000.0;
        eprintln!("display-facts fixture messages={size} records_ms={records_ms:.3} append_ms={append_ms:.3}");
        let materialize_started = std::time::Instant::now();
        materialize_all(&rig, &key);
        let materialize_ms = materialize_started.elapsed().as_secs_f64() * 1000.0;
        eprintln!("display-facts fixture messages={size} materialize_ms={materialize_ms:.3}");
        zero_ranges.store(0, Ordering::SeqCst);
        let now = std::time::Instant::now();
        let plain = rig
            .registry
            .read(read_request(&key, None, Some(32)))
            .await
            .unwrap();
        let default_ms = now.elapsed().as_secs_f64() * 1000.0;
        let default_full = zero_ranges.swap(0, Ordering::SeqCst);
        assert_eq!(plain.messages.last().unwrap().ordinal + 1, size);
        let lineage = plain.lineage_id.as_ref().unwrap();
        assert!(
            rig.store
                .read_display_facts_cache(&key, lineage, u64::MAX, &[])
                .unwrap()
                .unwrap()
                .seed
                .is_none(),
            "default read never starts display backfill"
        );
        let mut request = read_request(&key, None, Some(32));
        request.include_display_facts = true;
        let now = std::time::Instant::now();
        let first = rig.registry.read(request.clone()).await.unwrap();
        let backfill_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            zero_ranges.swap(0, Ordering::SeqCst),
            1,
            "one lazy lineage backfill"
        );
        let now = std::time::Instant::now();
        let warm = rig.registry.read(request).await.unwrap();
        let warm_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            zero_ranges.swap(0, Ordering::SeqCst),
            0,
            "warm opt-in reads no whole WAL"
        );
        assert_eq!(first, warm);
        let times = warm
            .messages
            .iter()
            .find(|entry| !entry.tool_times.is_empty())
            .unwrap();
        assert_eq!(
            serde_json::to_value(&times.tool_times).unwrap(),
            serde_json::json!([{"tool_call_id":"fact-call","started_at":100,"finished_at":150,"call_key":times.tool_calls[0].call_key}])
        );
        let reopened = broca_store::SqliteStore::open(rig.dir.join("store.db")).unwrap();
        assert!(
            reopened
                .read_display_facts_cache(&key, lineage, u64::MAX, &[])
                .unwrap()
                .unwrap()
                .seed
                .is_some(),
            "completion survives another store connection"
        );
        let now = std::time::Instant::now();
        let after = rig
            .registry
            .read(read_request(&key, None, Some(32)))
            .await
            .unwrap();
        let default_warm_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(plain, after);
        println!("display-facts messages={size} default_cold_ms={default_ms:.3} default_cold_full_reads={default_full} opt_in_backfill_ms={backfill_ms:.3} opt_in_warm_ms={warm_ms:.3} default_warm_ms={default_warm_ms:.3}");
        let mut next = started(&key, "next", "next prompt");
        if let WalRecord::RunStarted {
            request_identity_digest,
            ..
        } = &mut next
        {
            *request_identity_digest = Some(serde_json::json!({"version":1,"model":"","input":"","tools":"","policy_allowed":"","tool_choice":"","generation":"","cache":"","budget":"","stop_when":"","client_message_id":"phone_next"}).to_string());
        }
        append_seed(
            &rig.wal_store,
            &key,
            &[next, assistant_text(0, "next answer"), finished_ok()],
        );
        zero_ranges.store(0, Ordering::SeqCst);
        let mut request = read_request(&key, None, Some(2));
        request.include_display_facts = true;
        let extended = rig.registry.read(request).await.unwrap();
        assert_eq!(
            extended.messages[0].client_message_id.as_deref(),
            Some("phone_next")
        );
        assert_eq!(
            zero_ranges.load(Ordering::SeqCst),
            0,
            "a completed backfill continues from its byte anchor"
        );
    }
}
