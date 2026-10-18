//! Migration-only WAL reader. It has no writer, manifest, lease, extraction or
//! repair API, and every file is opened with `std::fs::File::open`.
use super::*;
use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom};

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
            let len = std::fs::metadata(&path).map_err(WalError::Io)?.len();
            return Ok((broca_wal::replay_file(path, DEFAULT_MAX_READ_BYTES)?, len));
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
