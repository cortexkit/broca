//! Opt-in corpus measurement; fixture creation is excluded from the sweep measurements.
use super::*;
use broca_wal::Wal;

fn cpu_ticks() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "creates 55,000 WALs; run explicitly for retention performance measurements"]
async fn retention_corpus_measurement() {
    let rig = Rig::new();
    fs::create_dir_all(rig.service.wal.ephemeral_root()).unwrap();
    for n in 0..55_000 {
        let key = BindIdentity::new(rig.root.join("project"), "runner", format!("session-{n}"));
        let seconds = if n < 100 { 1 } else { 3600 };
        let mut records = vec![started(&key, &format!("run-{n}"), Some(seconds), 1000)];
        if n < 50_000 {
            records.push(finished(1000));
        }
        let path = rig.service.wal.ephemeral_wal_path(&key);
        let mut wal = broca_wal::FileWal::open(&path)
            .unwrap()
            .with_feature_floor(rig.service.wal.feature_floor_path());
        for record in &records {
            wal.append(1, record, false).unwrap();
        }
        let frames: Vec<_> = records
            .into_iter()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect();
        let (row, runs) = derive(
            &key,
            wal.lineage_id().unwrap().to_string(),
            &frames,
            fs::metadata(&path).unwrap().len(),
            None,
        )
        .unwrap();
        rig.store.upsert_retention_lineage(&row, &runs).unwrap();
        fs::write(
            path.with_extension("session"),
            serde_json::to_vec(&key).unwrap(),
        )
        .unwrap();
    }
    let registry = rig.registry(None);
    for (label, now) in [
        ("boot", 0),
        ("resume", 1999),
        ("steady", 1999),
        ("due100", 2000),
    ] {
        std::io::stderr()
            .write_all(format!("RETENTION_BENCH_BEGIN {label}\n").as_bytes())
            .unwrap();
        let cpu = cpu_ticks();
        let wall = std::time::Instant::now();
        let count = if label == "boot" {
            registry.boot_retention().await.unwrap()
        } else if label == "resume" {
            rig.service.resume_deletions(now).unwrap().len()
        } else {
            registry.sweep_once(now).await.unwrap()
        };
        let elapsed = wall.elapsed();
        let ticks = cpu_ticks() - cpu;
        std::io::stderr()
            .write_all(
                format!(
                    "RETENTION_BENCH_END {label} wall_us={} cpu_ticks={ticks} count={count}\n",
                    elapsed.as_micros()
                )
                .as_bytes(),
            )
            .unwrap();
        if label == "due100" {
            assert_eq!(count, 100);
        }
    }
}
