//! Measures what one release returns after a synthetic heap peak, using the platform's own
//! accounting: `vmmap --summary` on macOS, `/proc/self/status` on Linux. The peak is many small
//! and medium allocations (the size classes a decoded conversation uses). One in 4,093 is kept,
//! a few scattered survivors, as after a wave of runs has finished.
//!
//! Run: `cargo run --release -p broca-memory-relief --example peak_release`
fn report(label: &str) {
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id().to_string();
        let out = std::process::Command::new("vmmap")
            .args(["--summary", &pid])
            .output()
            .expect("run vmmap");
        let text = String::from_utf8_lossy(&out.stdout);
        println!("== {label}");
        for line in text.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("Physical footprint")
                || trimmed.starts_with("MALLOC ZONE")
                || trimmed.starts_with("DefaultMallocZone")
            {
                println!("{line}");
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").expect("read status");
        println!("== {label}");
        for line in status
            .lines()
            .filter(|line| line.starts_with("VmRSS") || line.starts_with("RssAnon"))
        {
            println!("{line}");
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    println!("== {label} (no footprint source on this platform)");
}

fn main() {
    report("start");
    let sizes = [96usize, 700, 3_000, 24_000];
    let mut kept = Vec::new();
    {
        let mut peak: Vec<Vec<u8>> = Vec::new();
        let mut total = 0usize;
        let mut i = 0usize;
        while total < 256 * 1024 * 1024 {
            let size = sizes[i % sizes.len()];
            let block = vec![(i % 251) as u8; size];
            total += size;
            peak.push(block);
            i += 1;
        }
        report("peak: 256 MiB live");
        for (index, block) in peak.into_iter().enumerate() {
            if index % 4_093 == 0 {
                kept.push(block);
            }
        }
    }
    let kept_bytes: usize = kept.iter().map(Vec::len).sum();
    report(&format!("after free: {kept_bytes} bytes still live"));
    let started = std::time::Instant::now();
    let released = broca_memory_relief::release_freed_memory();
    let elapsed = started.elapsed();
    println!("release returned {released:?} in {elapsed:?}");
    report("after release");
    std::hint::black_box(&kept);
}
