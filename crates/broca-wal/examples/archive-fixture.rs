//! Build sealed fixtures for the Python diagnostic tests with the production writer.
//! Usage: archive-fixture <container> <wal-file> [<wal-file> ...]
#![forbid(unsafe_code)]

use broca_wal::archive::ArchiveWriter;
use std::{io, path::PathBuf};

fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let output = args.next().expect("container path required");
    // A fixture outside any state root: there is no feature floor to raise.
    let mut writer = ArchiveWriter::create(output)?.without_feature_floor();
    for arg in args {
        let path = PathBuf::from(arg);
        let address = path.file_stem().and_then(|s| s.to_str()).expect("WAL stem");
        writer.append(address, &std::fs::read(&path)?)?;
    }
    writer.seal()
}
