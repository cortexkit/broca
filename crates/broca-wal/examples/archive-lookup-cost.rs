//! Read-only lookup measurement on an existing archive directory.
#[cfg(feature = "test-io-trace")]
use broca_wal::archive::{expiry_io, read_newest_member, ArchiveReader};
#[cfg(feature = "test-io-trace")]
use std::{collections::BTreeSet, io, path::PathBuf, time::Instant};

#[cfg(not(feature = "test-io-trace"))]
fn main() {
    eprintln!("enable --features test-io-trace for lookup accounting");
}

#[cfg(feature = "test-io-trace")]
fn main() -> io::Result<()> {
    let root = PathBuf::from(std::env::args_os().nth(1).expect("archive directory"));
    let mut paths = std::fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "ark"));
    paths.sort();
    let mut addresses = BTreeSet::new();
    for path in &paths {
        let mut reader = ArchiveReader::open(path, u64::MAX)?;
        if let Some(members) = reader.verified_members()? {
            addresses.extend(members.into_iter().map(|member| member.session_addr));
        } else {
            addresses.extend(
                reader
                    .rescan()?
                    .into_iter()
                    .map(|member| member.session_addr),
            );
        }
    }
    println!("containers={} addresses={}", paths.len(), addresses.len());
    let before = expiry_io::counts().0;
    let start = Instant::now();
    let first = addresses.first().expect("nonempty archive");
    assert!(read_newest_member(&paths, first, u64::MAX)?.is_some());
    println!(
        "first_lookup_us={} opens={}",
        start.elapsed().as_micros(),
        expiry_io::counts().0 - before
    );
    #[cfg(unix)]
    println!(
        "member_index={:?}",
        broca_wal::archive::cached_member_index_stats(paths[0].parent().unwrap())
    );
    let before = expiry_io::counts().0;
    let start = Instant::now();
    let mut times = Vec::new();
    let mut bytes = 0;
    for addr in &addresses {
        let lookup = Instant::now();
        let member = read_newest_member(&paths, addr, u64::MAX)?.expect("inventoried member");
        bytes += member.payload.len();
        times.push(lookup.elapsed().as_micros());
    }
    let elapsed = start.elapsed();
    times.sort_unstable();
    println!(
        "sweep_ms={} opens={} bytes={} median_us={} p95_us={} mean_us={}",
        elapsed.as_millis(),
        expiry_io::counts().0 - before,
        bytes,
        times[times.len() / 2],
        times[times.len() * 95 / 100],
        elapsed.as_micros() / times.len() as u128
    );
    Ok(())
}
