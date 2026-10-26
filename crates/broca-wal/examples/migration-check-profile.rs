//! Synthetic measurement of ordinary write-fence checks, excluding move fsyncs.
// This non-shipping profiler directly prepares and removes its synthetic fixture.
#![allow(clippy::disallowed_methods)]

use broca_wal::migration::{atomic_json, check, rebuild_fence_index};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("broca-fence-profile-{}", std::process::id()));
    std::fs::create_dir_all(root.join("wal"))?;
    for i in 0..128 {
        atomic_json(
            &root,
            &root.join(format!("agent-data/cuts/{i:032x}.json")),
            &serde_json::json!({"intent":null,"replies":{"status":"x".repeat(8192)}}),
        )?;
    }
    let wal = root.join("wal/session.wal");
    let reference = "00000000000000000000000000000000";
    let iterations = 2000;
    let start = Instant::now();
    for _ in 0..iterations {
        assert!(check(&wal, reference, true)?.is_ok());
    }
    let before = start.elapsed().as_micros() as f64 / iterations as f64;
    rebuild_fence_index(&root)?;
    let start = Instant::now();
    for _ in 0..iterations {
        assert!(check(&wal, reference, true)?.is_ok());
    }
    let after = start.elapsed().as_micros() as f64 / iterations as f64;
    println!("cuts=128 iterations={iterations} before_us={before:.3} after_us={after:.3}");
    std::fs::remove_dir_all(root)?;
    Ok(())
}
