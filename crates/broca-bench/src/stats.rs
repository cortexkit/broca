//! Percentile + jitter statistics over a sample of nanosecond latencies.

/// Summary statistics for a latency sample, all in nanoseconds.
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

impl LatencyStats {
    /// Compute percentile stats from a latency sample (consumes-sorts a copy).
    pub fn from_samples(samples: &[u64]) -> Self {
        assert!(!samples.is_empty(), "latency sample must be non-empty");
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let n = sorted.len();
        let pct = |p: f64| -> u64 {
            // Nearest-rank percentile.
            let rank = (p / 100.0 * n as f64).ceil() as usize;
            let idx = rank.saturating_sub(1).min(n - 1);
            sorted[idx]
        };
        LatencyStats {
            p50: pct(50.0),
            p95: pct(95.0),
            p99: pct(99.0),
            max: sorted[n - 1],
        }
    }
}

/// Jitter = standard deviation of consumer inter-arrival gaps around the
/// injected cadence. `gaps_ns` are successive recv-to-recv deltas; `cadence_ns`
/// is the target spacing. Returns (stddev_ns, mean_abs_dev_ns).
pub fn jitter(gaps_ns: &[u64], cadence_ns: u64) -> (f64, f64) {
    if gaps_ns.is_empty() {
        return (0.0, 0.0);
    }
    let target = cadence_ns as f64;
    let devs: Vec<f64> = gaps_ns.iter().map(|&g| g as f64 - target).collect();
    let mean_dev = devs.iter().sum::<f64>() / devs.len() as f64;
    let var = devs.iter().map(|d| (d - mean_dev).powi(2)).sum::<f64>() / devs.len() as f64;
    let mean_abs = devs.iter().map(|d| d.abs()).sum::<f64>() / devs.len() as f64;
    (var.sqrt(), mean_abs)
}

/// Mean latency per time-bucket (samples split into `buckets` equal-count
/// slices, in arrival order). Reveals cumulative drift: a rising trend means the
/// consumer is falling behind (queue buildup). Returns means in ns.
pub fn bucket_means(lat_ns: &[u64], buckets: usize) -> Vec<f64> {
    if lat_ns.is_empty() || buckets == 0 {
        return Vec::new();
    }
    let n = lat_ns.len();
    let per = n.div_ceil(buckets);
    lat_ns
        .chunks(per)
        .map(|c| c.iter().map(|&v| v as f64).sum::<f64>() / c.len() as f64)
        .collect()
}
