//! Migration-only WAL reader. It has no writer, manifest, lease, extraction or
//! repair API, and every file is opened with `std::fs::File::open`.
use super::*;
use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom};

#[cfg(test)]
thread_local! {
    /// Runs once after `records` resolves a live path and before it reads the file, so a test
    /// can unlink the file in that window.
    static AFTER_LIVE_PATH_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub struct ReadOnlyWalReader {
    root: PathBuf,
}
impl ReadOnlyWalReader {
    pub fn new(state_root: impl Into<PathBuf>) -> Self {
        Self {
            root: state_root.into(),
        }
    }
    fn live_path(&self, address: &str) -> Result<Option<PathBuf>, WalError> {
        if address.len() != 16 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(WalError::Io(std::io::Error::other("invalid WAL address")));
        }
        let mut found = None;
        for name in ["wal", "wal-ephemeral", "wal-durable"] {
            let path = self.root.join(name).join(format!("{address}.wal"));
            if broca_wal::wal_file_exists(&path)? {
                if found.is_some() {
                    return Err(WalError::Io(std::io::Error::other(
                        "ambiguous live WAL address",
                    )));
                }
                found = Some(path);
            }
        }
        Ok(found)
    }
    fn containers(&self, name: &str) -> Result<Vec<PathBuf>, WalError> {
        let mut paths = Vec::new();
        match std::fs::read_dir(self.root.join(name)) {
            Ok(entries) => {
                for entry in entries {
                    let path = entry.map_err(WalError::Io)?.path();
                    if path.extension().is_some_and(|ext| ext == "ark") {
                        paths.push(path);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(WalError::Io(error)),
        }
        Ok(paths)
    }
    pub fn addresses(&self) -> Result<BTreeSet<String>, WalError> {
        let mut addresses = BTreeSet::new();
        for name in ["wal", "wal-ephemeral", "wal-durable"] {
            match std::fs::read_dir(self.root.join(name)) {
                Ok(entries) => {
                    for entry in entries {
                        let path = entry.map_err(WalError::Io)?.path();
                        if path.extension().is_some_and(|ext| ext == "wal") {
                            let address =
                                path.file_stem().and_then(|name| name.to_str()).ok_or_else(
                                    || WalError::Io(std::io::Error::other("invalid WAL filename")),
                                )?;
                            addresses.insert(address.to_owned());
                        }
                        std::thread::yield_now();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WalError::Io(error)),
            }
        }
        for address in &addresses {
            self.live_path(address)?;
            std::thread::yield_now();
        }
        for name in ["wal-archive", "wal-archive-durable"] {
            for path in self.containers(name)? {
                let mut reader =
                    broca_wal::archive::ArchiveReader::open(path, DEFAULT_MAX_READ_BYTES)
                        .map_err(WalError::Io)?;
                if reader.state().map_err(WalError::Io)?
                    == broca_wal::archive::ContainerState::Unsealed
                {
                    continue;
                }
                if let Some(members) = reader.verified_members().map_err(WalError::Io)? {
                    addresses.extend(members.into_iter().map(|member| member.session_addr));
                } else {
                    addresses.extend(
                        reader
                            .rescan()
                            .map_err(WalError::Io)?
                            .into_iter()
                            .map(|member| member.session_addr),
                    );
                }
                std::thread::yield_now();
            }
        }
        Ok(addresses)
    }
    pub fn records(&self, address: &str) -> Result<(Vec<broca_wal::FramedRecord>, u64), WalError> {
        if let Some(path) = self.live_path(address)? {
            #[cfg(test)]
            if let Some(hook) = AFTER_LIVE_PATH_HOOK.with(|hook| hook.borrow_mut().take()) {
                hook();
            }
            // One open handle answers both presence and content. A file unlinked after
            // `live_path` (a fold archiving it) reads as absent, never as an empty WAL, so the
            // lookup continues to the archives and, failing them, reports disappearance below.
            let range = broca_wal::file_wal::read_records_range(
                &path,
                DEFAULT_MAX_READ_BYTES,
                0,
                None,
                None,
            )?;
            let len = range.complete_byte_end.unwrap_or(0);
            match range.into_records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => return Ok((records, len)),
                broca_wal::WalRangeRecords::Absent => {}
            }
        }
        let ordinary = broca_wal::archive::read_newest_member(
            &self.containers("wal-archive")?,
            address,
            DEFAULT_MAX_READ_BYTES,
        )
        .map_err(WalError::Io)?;
        let durable = broca_wal::archive::read_newest_member(
            &self.containers("wal-archive-durable")?,
            address,
            DEFAULT_MAX_READ_BYTES,
        )
        .map_err(WalError::Io)?;
        if ordinary.is_some() && durable.is_some() {
            return Err(WalError::Io(std::io::Error::other(
                "ambiguous archived WAL address",
            )));
        }
        if let Some(member) = durable.or(ordinary) {
            let len = member.payload.len() as u64;
            let range =
                broca_wal::file_wal::read_verified_member_range(&member.payload, 0, 0, None)?;
            if let broca_wal::WalRangeRecords::Complete(records) = range.into_records() {
                return Ok((records, len));
            }
        }
        Err(WalError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "census address disappeared; retry metadata enumeration before proving absence",
        )))
    }
    /// Only lineage metadata is read for an already represented live address.
    pub fn probe(&self, address: &str) -> Result<u64, WalError> {
        use broca_wal::framing::{self, DecodeStep};
        let Some(path) = self.live_path(address)? else {
            return Ok(0);
        };
        let mut file = std::fs::File::open(path).map_err(WalError::Io)?;
        let len = file.metadata().map_err(WalError::Io)?.len();
        let mut head = [0; framing::HEADER_LEN + 16];
        let mut read = 0;
        while read < head.len() {
            let n = file.read(&mut head[read..]).map_err(WalError::Io)?;
            if n == 0 {
                break;
            }
            read += n;
        }
        let head_id = match framing::decode_next(&head[..read]) {
            DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id),
            _ => None,
        };
        let mut bytes = read as u64;
        if len >= head.len() as u64 {
            file.seek(SeekFrom::Start(len - head.len() as u64))
                .map_err(WalError::Io)?;
            file.read_exact(&mut head).map_err(WalError::Io)?;
            bytes += head.len() as u64;
            if let DecodeStep::Lineage { lineage_id, .. } = framing::decode_next(&head) {
                if head_id.is_some_and(|head| head != lineage_id) {
                    return Err(WalError::Io(std::io::Error::other(
                        "WAL head and tail lineage disagree",
                    )));
                }
            }
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalStore;

    /// A census-only reader over one live WAL holding a single record.
    fn live_wal(name: &str) -> (PathBuf, String, PathBuf) {
        let root = broca_test_support::scratch_root()
            .join(format!("projection-reader-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let bind = BindIdentity::new("/project", "census", name);
        let store = FileWalStore::new(root.join("wal"));
        let mut wal = store.open(&bind).unwrap();
        wal.append(
            1,
            &WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        drop(wal);
        (root.clone(), session_addr(&bind), store.wal_path(&bind))
    }

    #[test]
    fn live_wal_reads_its_records() {
        let (root, address, _) = live_wal("present");
        let (records, bytes) = ReadOnlyWalReader::new(&root).records(&address).unwrap();
        assert_eq!(records.len(), 1);
        assert!(bytes > 0);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A fold can unlink a live WAL after the census resolved its path. That read must report
    /// the disappearance so the census retries enumeration; an empty record set would certify
    /// the address as covered with none of its session refs.
    #[test]
    fn live_wal_unlinked_after_resolution_is_not_an_empty_read() {
        let (root, address, path) = live_wal("vanished");
        AFTER_LIVE_PATH_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || std::fs::remove_file(path).unwrap()));
        });
        let read = ReadOnlyWalReader::new(&root).records(&address);
        match read {
            Err(WalError::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
            other => panic!("a vanished live WAL must not read as empty: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
