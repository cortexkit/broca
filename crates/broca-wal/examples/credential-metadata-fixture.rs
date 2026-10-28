//! Write deploy-check fixtures through the production record serializer and FileWal.
//! Usage: credential-metadata-fixture <wal-file> < records.json
#![forbid(unsafe_code)]

use broca_wal::{FileWal, Wal, WalRecord};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(std::env::args_os().nth(1).expect("WAL path required"));
    let records: Vec<WalRecord> = serde_json::from_reader(std::io::stdin().lock())?;
    let mut wal = FileWal::open(&output)?.with_feature_floor(output.with_extension("features"));
    for record in records {
        assert_ne!(record, WalRecord::Unknown, "fixture must be a known record");
        wal.append(1, &record, true)?;
    }
    Ok(())
}
