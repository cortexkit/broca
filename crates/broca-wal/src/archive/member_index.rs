//! Directory-wide newest-member locations. No open file or payload is retained.
//!
//! Path discovery remains the caller's responsibility. Every lookup stats the supplied
//! containers, including those that previously missed: an external rewrite can add a
//! newer copy of an address to any one of them. Only an unchanged snapshot can answer
//! absence or skip a container. Uncertainty always returns to the authoritative scan.
use super::*;
use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::MetadataExt,
    sync::{Arc, Mutex, OnceLock},
};

const BUDGET: usize = 64 << 20;
const MAX_ADDRESSES: usize = BUDGET / (std::mem::size_of::<([u8; 16], Location)>() + 1);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    dev: u64,
    ino: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl Identity {
    fn of(meta: std::fs::Metadata) -> Self {
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.len(),
            modified: (meta.mtime(), meta.mtime_nsec()),
            changed: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

#[derive(PartialEq, Eq)]
struct Container {
    path: PathBuf,
    identity: Identity,
}

#[derive(Clone, Copy)]
struct Location {
    container: usize,
    offset: u64,
    payload_len: u64,
}

struct Index {
    containers: Vec<Container>,
    newest: HashMap<[u8; 16], Location>,
    restored: Mutex<HashSet<[u8; 16]>>,
}

impl Index {
    fn bytes(&self) -> usize {
        // HashMap capacity excludes its spare buckets. Round up for the backing
        // allocation and include one control byte per bucket plus the path storage.
        self.newest.capacity().next_power_of_two()
            * (std::mem::size_of::<([u8; 16], Location)>() + 1)
            + self.containers.capacity() * std::mem::size_of::<Container>()
            + self
                .containers
                .iter()
                .map(|item| item.path.as_os_str().len())
                .sum::<usize>()
            + std::mem::size_of::<Self>()
            + 16
            + self
                .restored
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .capacity()
                .next_power_of_two()
                * (std::mem::size_of::<[u8; 16]>() + 1)
    }

    fn build(containers: Vec<Container>) -> io::Result<Option<Self>> {
        let mut newest = HashMap::new();
        // The snapshot is ordered newest first; the first occurrence owns the address.
        for (container, item) in containers.iter().enumerate() {
            let mut reader = ArchiveReader::open(&item.path, u64::MAX)?;
            if Identity::of(reader.source.get_ref().metadata()?) != item.identity {
                return Ok(None);
            }
            if reader.state()? == ContainerState::Unsealed {
                continue;
            }
            // Only a digest-verified index proves completeness without checking headers.
            // Old or damaged indexes retain their header-walk recovery semantics.
            let members = match reader.verified_members()? {
                Some(members) => members,
                None => {
                    let mut headers = reader.rescan()?;
                    headers.sort_unstable_by(|a, b| a.session_addr.cmp(&b.session_addr));
                    if let Some(IndexOutcome::Unverified { entries }) = &reader.index {
                        // An unverified index hit is still checked against its header by
                        // the scan path. Do not hide an index/header disagreement by
                        // silently substituting the header's correct location.
                        if entries.len() != headers.len()
                            || entries.iter().zip(&headers).any(|(entry, header)| {
                                header.session_addr.as_bytes() != entry.address
                                    || header.offset != entry.offset
                                    || header.payload_len != entry.payload_len
                            })
                        {
                            return Ok(None);
                        }
                    }
                    headers
                        .into_iter()
                        .map(|info| IndexedMember {
                            session_addr: info.session_addr,
                            offset: info.offset,
                            payload_len: info.payload_len,
                        })
                        .collect()
                }
            };
            for member in members {
                newest
                    .entry(address(&member.session_addr)?)
                    .or_insert(Location {
                        container,
                        offset: member.offset,
                        payload_len: member.payload_len,
                    });
                if newest.len() > MAX_ADDRESSES {
                    return Ok(None);
                }
            }
        }
        // A replacement during construction must never publish a mixed-generation index.
        if snapshot(
            &containers
                .iter()
                .map(|item| item.path.clone())
                .collect::<Vec<_>>(),
        )? != containers
        {
            return Ok(None);
        }
        Ok(Some(Self {
            containers,
            newest,
            restored: Mutex::default(),
        }))
    }
}

fn snapshot(paths: &[PathBuf]) -> io::Result<Vec<Container>> {
    let mut ordered = paths
        .iter()
        .map(|path| Ok((fold_timestamp(path)?, path)))
        .collect::<io::Result<Vec<_>>>()?;
    ordered.sort_unstable_by_key(|(stamp, _)| std::cmp::Reverse(*stamp));
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid("ambiguous archive timestamps"));
    }
    ordered
        .into_iter()
        .map(|(_, path)| {
            Ok(Container {
                path: path.clone(),
                identity: Identity::of(crate::fs_ops::metadata(path)?),
            })
        })
        .collect()
}

fn cache() -> &'static Mutex<HashMap<PathBuf, Arc<Index>>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, Arc<Index>>>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

#[cfg(any(test, feature = "test-io-trace", feature = "test-known-feature"))]
pub(super) fn stats(directory: &Path) -> Option<(usize, usize, usize)> {
    let cache = cache().lock().unwrap_or_else(|e| e.into_inner());
    let index = cache.get(directory)?;
    Some((index.containers.len(), index.newest.len(), index.bytes()))
}

pub(super) fn invalidate(directory: &Path) {
    cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(directory);
}

pub(super) fn restored(directory: &Path, addr: &str) {
    let Ok(addr) = address(addr) else {
        invalidate(directory);
        return;
    };
    let mut cache = cache().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(index) = cache.get(directory) {
        index
            .restored
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(addr);
        if cache.values().map(|index| index.bytes()).sum::<usize>() > BUDGET {
            cache.remove(directory);
        }
    }
}

/// Outer `None` means uncertainty: the caller must run the original scan, not return a miss.
pub(super) fn lookup(
    paths: &[PathBuf],
    addr: &str,
    max_bytes: u64,
) -> io::Result<Option<Option<ArchiveMember>>> {
    let addr_bytes = address(addr)?;
    let Some(parent) = paths.first().and_then(|path| path.parent()) else {
        return Ok(None);
    };
    if paths.iter().any(|path| path.parent() != Some(parent)) {
        return Ok(None);
    }
    let mut cache = cache().lock().unwrap_or_else(|e| e.into_inner());
    let current = match snapshot(paths) {
        Ok(current) => current,
        Err(_) => {
            cache.remove(parent);
            return Ok(None);
        }
    };
    let index = if let Some(index) = cache.get(parent) {
        if index.containers != current {
            cache.remove(parent);
            return Ok(None);
        }
        Arc::clone(index)
    } else {
        // Serialize construction, but release the lock before reading any payload.
        let Some(index) = Index::build(current).ok().flatten() else {
            return Ok(None);
        };
        let index = Arc::new(index);
        let bytes = index.bytes();
        if bytes > BUDGET {
            return Ok(None);
        }
        if cache.values().map(|index| index.bytes()).sum::<usize>() + bytes > BUDGET {
            cache.clear();
        }
        cache.insert(parent.to_owned(), Arc::clone(&index));
        index
    };
    drop(cache);
    if index
        .restored
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&addr_bytes)
    {
        return Ok(None);
    }
    let Some(location) = index.newest.get(&addr_bytes) else {
        return Ok(Some(None));
    };
    let item = &index.containers[location.container];
    let answer: io::Result<Option<Option<ArchiveMember>>> = (|| {
        let mut reader = ArchiveReader::open(&item.path, max_bytes)?;
        if Identity::of(reader.source.get_ref().metadata()?) != item.identity {
            return Ok(None);
        }
        let member = reader.read_located(addr, location.offset, location.payload_len)?;
        if Identity::of(reader.source.get_ref().metadata()?) != item.identity {
            return Ok(None);
        }
        Ok(Some(Some(member)))
    })();
    if !matches!(answer, Ok(Some(_))) {
        invalidate(parent);
    }
    // Opening races, I/O failures and changed headers retain the scan's error/NotFound
    // behavior. In particular, uncertainty must never turn into a cached negative.
    match answer {
        Ok(answer) => Ok(answer),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests;
