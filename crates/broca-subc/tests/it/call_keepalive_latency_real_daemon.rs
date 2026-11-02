//! Manual real-daemon timing probe for optional keepalive discovery.
//! Run explicitly with `cargo nextest run --run-ignored only` and the matching test filter.
use broca_fetch_plan::Fetcher;
use broca_protocol::BindIdentity;
use broca_subc::SubcPlanFetcher;
use std::{sync::Arc, time::Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual real-daemon catalog timing probe"]
async fn measure_fresh_and_reused_admission_catalog_snapshots() {
    let Some(rig) = super::common::Rig::start(
        "keepalive-latency",
        &[("FAKE_AFT_MODULE_ID", "aft"), ("FAKE_AFT_TOOLS", "read")],
    )
    .await
    else {
        return;
    };
    let conn = Arc::new(rig.connect().await);
    let mut fresh = Vec::new();
    let mut reused = Vec::new();
    for sample in 0..100 {
        let fetcher = SubcPlanFetcher::new(
            conn.clone(),
            BindIdentity::new(&rig.dir, "catalog-timing", format!("sample-{sample}")),
            None,
        );
        let started = Instant::now();
        fetcher
            .call_keepalive_supported("aft")
            .await
            .expect("catalog lookup succeeds");
        fresh.push(started.elapsed().as_micros());
        let started = Instant::now();
        fetcher
            .system_text_default_supported("aft")
            .await
            .expect("shared snapshot succeeds");
        reused.push(started.elapsed().as_micros());
        fetcher.close().await;
    }
    fresh.sort_unstable();
    reused.sort_unstable();
    println!("catalog snapshot native fixture, n=100: fresh_us p50={} p95={} max={}; reused_us p50={} p95={} max={}",
        fresh[49], fresh[94], fresh[99], reused[49], reused[94], reused[99]);
}
