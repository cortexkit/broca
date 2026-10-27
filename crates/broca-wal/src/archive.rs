//! Immutable containers of verbatim WAL files. The trailing index is a cache;
//! member headers are sufficient to rebuild it, but only a trailing seal permits reads.
//!
//! Removing an index for recovery must preserve the final eight bytes (use index_len = 0).
//! Removing the seal as well makes the file indistinguishable from a crashed fold, so it
//! must remain unreadable. This module never repairs or publishes such a file.

use crate::fs_ops::{File, OpenOptions};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub const MAGIC_MEMBER: u32 = u32::from_le_bytes(*b"WALM");
pub const MAGIC: u32 = u32::from_le_bytes(*b"WALK");
pub const FORMAT_VERSION: u16 = 1;
pub const MEMBER_HEADER_LEN: u64 = 64;
const INDEX_ENTRY_LEN: u64 = 32;

// Index format v2 appends a 64-byte extension inside the region `index_len` counts:
//
//   count | entries (sorted by address) | digest [32] | tag "IDX2" [4] | reserved [28] = 0
//   | index_len | MAGIC
//
// where index_len = 4 + 32 * count + 64 and the digest is
//
//   SHA-256("broca-ark-index/v2\0" || stamp u64 LE || index_len u32 LE || count || entries
//           || tag || reserved)
//
// over the exact bytes on disk, binding the stamp parsed from the container's file name.
// A v2 index whose digest verifies is authoritative for ABSENCE: a lookup it misses is
// not in the container, with no header scan. A hit is still checked against the member
// header and the payload digest, exactly as for v1.
//
// Readers that predate v2 fail its count equation, treat the index as damaged and fall
// back to the header scan, which stops at the index's u32 `count` (never equal to
// MAGIC_MEMBER) and so never reaches the extension. The trailer MAGIC, MAGIC_MEMBER and
// the member FORMAT_VERSION are therefore unchanged by v2: changing any of them would make
// those readers skip sealed containers or refuse every member. The tag must stay non-hex,
// so a count damaged to `count + 2` makes a v1 parse read the tag as an invalid address.
// The tag and reserved zeros sit next to the trailer, so a write torn inside the
// extension leaves zeros or a tag before `index_len`, never a digest that could match.
pub(crate) const INDEX_V2_EXTENSION_LEN: u64 = 64;
pub(crate) const INDEX_V2_TAG: [u8; 4] = *b"IDX2";
const INDEX_V2_DOMAIN: &[u8] = b"broca-ark-index/v2\0";

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn address(addr: &str) -> io::Result<[u8; 16]> {
    let bytes: [u8; 16] = addr
        .as_bytes()
        .try_into()
        .map_err(|_| invalid("session address must be 16 ASCII hex bytes"))?;
    if !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid("session address must be 16 ASCII hex bytes"));
    }
    Ok(bytes)
}

fn header_in_bounds(offset: u64, end: u64) -> io::Result<()> {
    if offset
        .checked_add(MEMBER_HEADER_LEN)
        .is_none_or(|next| next > end)
    {
        return Err(invalid("truncated archive member header"));
    }
    Ok(())
}

/// Check that index entries, in offset order, tile the member region `[0, start)` exactly:
/// the first starts at 0, each next one starts where the previous member ends, the last
/// ends at the index, and no address repeats. Lookups depend on this: a malformed or
/// missing entry must cost a header scan, never hide a session.
fn check_coverage(entries: &[IndexEntry], start: u64) -> Result<(), &'static str> {
    let mut next = 0u64;
    let mut seen = BTreeSet::new();
    for entry in entries {
        if entry.offset != next || !seen.insert(entry.address) {
            return Err("invalid archive index coverage");
        }
        next = entry
            .offset
            .checked_add(MEMBER_HEADER_LEN)
            .and_then(|n| n.checked_add(entry.payload_len))
            .filter(|n| *n <= start)
            .ok_or("index member exceeds bounds")?;
    }
    if next != start {
        return Err("archive index omits members");
    }
    Ok(())
}

/// Validate the 64-byte member header found at `offset`, in a member region ending at
/// `end`. Every header check lives here: the sequential header walk, the indexed read
/// and the membership cache's index-driven check all use it.
fn parse_header(
    bytes: &[u8; MEMBER_HEADER_LEN as usize],
    offset: u64,
    end: u64,
) -> io::Result<MemberInfo> {
    header_in_bounds(offset, end)?;
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != MAGIC_MEMBER
        || u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != FORMAT_VERSION
        || bytes[6..8] != [0, 0]
    {
        return Err(invalid("invalid archive member header"));
    }
    let addr =
        std::str::from_utf8(&bytes[8..24]).map_err(|_| invalid("invalid session address"))?;
    address(addr)?;
    let payload_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    // `header_in_bounds` has proved `offset + MEMBER_HEADER_LEN` cannot overflow.
    if (offset + MEMBER_HEADER_LEN)
        .checked_add(payload_len)
        .is_none_or(|next| next > end)
    {
        return Err(invalid("archive member exceeds container bounds"));
    }
    Ok(MemberInfo {
        session_addr: addr.to_owned(),
        offset,
        payload_len,
        payload_sha256: bytes[32..64].try_into().unwrap(),
    })
}

/// A header location, not proof that the payload's digest is valid. Offsets point to
/// the member header; lengths count payload bytes only, in both headers and indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInfo {
    pub session_addr: String,
    pub offset: u64,
    pub payload_len: u64,
    pub payload_sha256: [u8; 32],
}

/// One entry of a verified index: a member's address, the offset of its header, and the
/// length of its payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedMember {
    pub session_addr: String,
    pub offset: u64,
    pub payload_len: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ArchiveMember {
    pub info: MemberInfo,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    Sealed,
    Unsealed,
}

/// A fresh pass exclusively owns a newly created file. There is deliberately no
/// reopen-for-append API: appending past a seal would turn its index into member data.
/// Any write failure poisons the writer, preventing a partial member from being sealed.
///
/// Containers are sealed with index format v2, whose digest binds the stamp in the file
/// name, so the name must be the canonical `fold-<stamp>.ark`.
pub struct ArchiveWriter {
    file: File,
    path: PathBuf,
    stamp: u64,
    entries: Vec<MemberInfo>,
    failed: bool,
    floor: Floor,
}

/// What `seal` does about the WAL feature floor before writing a v2 index.
enum Floor {
    /// The caller has not chosen between the two variants below. `seal` refuses, so a
    /// caller cannot seal a v2 index merely by forgetting to raise the floor.
    Unset,
    /// Add `archive-index/v2` to the floor file at this path, durably, before writing any
    /// index byte.
    Raise(PathBuf),
    /// The caller declared that the container belongs to no broca state root, so there is
    /// no floor file to raise.
    None,
}

impl ArchiveWriter {
    /// The parent directory must already exist; its durability belongs to the caller.
    /// Creation uses owner-only permissions, like the live WAL files.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let stamp = fold_timestamp(path).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("archive container must be named fold-<stamp>.ark: {error}"),
            )
        })?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        Ok(Self {
            file: options.open(path)?,
            path: path.to_owned(),
            stamp,
            entries: Vec::new(),
            failed: false,
            floor: Floor::Unset,
        })
    }

    /// Raise the WAL feature floor at `floor` (the state root's
    /// [`FEATURE_FLOOR_FILENAME`](crate::FEATURE_FLOOR_FILENAME)) with
    /// `archive-index/v2` before the seal writes its index. `scripts/place-release.sh` reads
    /// the floor and refuses to install a build that does not list every name in it, so once
    /// the name is there no build that cannot read v2 indexes is installed. Sealing first
    /// could leave a v2 container on a machine that then rolls back to such a build.
    pub fn with_feature_floor(mut self, floor: impl Into<PathBuf>) -> Self {
        self.floor = Floor::Raise(floor.into());
        self
    }

    /// Seal without raising any feature floor. Only for containers no state root governs:
    /// tools and tests that build a container outside a broca install.
    pub fn without_feature_floor(mut self) -> Self {
        self.floor = Floor::None;
        self
    }

    pub fn append(&mut self, session_addr: &str, payload: &[u8]) -> io::Result<MemberInfo> {
        if self.failed {
            return Err(invalid("archive writer is poisoned"));
        }
        let addr = address(session_addr)?;
        if self
            .entries
            .iter()
            .any(|entry| entry.session_addr == session_addr)
        {
            return Err(invalid("duplicate session in one container"));
        }
        // The trailer has a u32 index length, so refuse an unsealable member count.
        if v2_index_len(self.entries.len() + 1).is_none() {
            return Err(invalid("archive index exceeds u32 length"));
        }
        self.failed = true;
        let info = MemberInfo {
            session_addr: session_addr.to_owned(),
            offset: self.file.stream_position()?,
            payload_len: payload.len() as u64,
            payload_sha256: Sha256::digest(payload).into(),
        };
        let mut header = [0; MEMBER_HEADER_LEN as usize];
        header[0..4].copy_from_slice(&MAGIC_MEMBER.to_le_bytes());
        header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[8..24].copy_from_slice(&addr);
        header[24..32].copy_from_slice(&info.payload_len.to_le_bytes());
        header[32..64].copy_from_slice(&info.payload_sha256);
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;
        self.entries.push(info.clone());
        self.failed = false;
        Ok(info)
    }

    /// Make the members written so far durable, THROUGH THE WRITER'S OWN HANDLE.
    ///
    /// A caller must not reach for `File::open(path)?.sync_all()` instead: that yields a
    /// READ-ONLY handle, and Windows `FlushFileBuffers` requires write access, so it fails
    /// with `PermissionDenied` while succeeding silently on unix. The writer owns the file,
    /// so it owns the barrier.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Consumes the only writable handle, so neither a second seal nor a later append
    /// is expressible. A successful return includes fsync of the index and seal.
    ///
    /// Writes index format v2: the entries sorted by address, then the extension (digest,
    /// tag, reserved zeros), then `index_len` and `MAGIC`, in one pass followed by one
    /// fsync. A crash can leave any subset of those pages durable, but a torn index never
    /// verifies, and fold finish re-reads every member before unlinking its live source.
    /// Refuses unless the caller chose [`with_feature_floor`](Self::with_feature_floor) or
    /// [`without_feature_floor`](Self::without_feature_floor); a floor is raised first.
    ///
    /// ```compile_fail,E0382
    /// use broca_wal::archive::ArchiveWriter;
    /// let mut writer = ArchiveWriter::create("fold-1.ark").unwrap();
    /// writer.seal().unwrap();
    /// writer.append("0123456789abcdef", b"later").unwrap();
    /// ```
    pub fn seal(mut self) -> io::Result<()> {
        if self.failed {
            return Err(invalid("archive writer is poisoned"));
        }
        match &self.floor {
            Floor::Unset => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "archive writer has no feature floor decision; refusing to seal a v2 index",
                ))
            }
            Floor::Raise(floor) => {
                crate::floor::raise_floor(floor, &[crate::framing::ARCHIVE_INDEX_V2_FEATURE])?
            }
            Floor::None => {}
        }
        let count = u32::try_from(self.entries.len())
            .map_err(|_| invalid("archive index exceeds u32 length"))?;
        let index_len = v2_index_len(self.entries.len())
            .ok_or_else(|| invalid("archive index exceeds u32 length"))?;
        self.entries
            .sort_unstable_by(|a, b| a.session_addr.cmp(&b.session_addr));
        let mut index = Vec::with_capacity(index_len as usize + 8);
        index.extend_from_slice(&count.to_le_bytes());
        for entry in &self.entries {
            index.extend_from_slice(entry.session_addr.as_bytes());
            index.extend_from_slice(&entry.offset.to_le_bytes());
            index.extend_from_slice(&entry.payload_len.to_le_bytes());
        }
        let mut fixed = [0; 32];
        fixed[..4].copy_from_slice(&INDEX_V2_TAG);
        // Hash exactly the bytes written below.
        let mut hasher = Sha256::new();
        hasher.update(INDEX_V2_DOMAIN);
        hasher.update(self.stamp.to_le_bytes());
        hasher.update(index_len.to_le_bytes());
        hasher.update(&index);
        hasher.update(fixed);
        index.extend_from_slice(&hasher.finalize());
        index.extend_from_slice(&fixed);
        index.extend_from_slice(&index_len.to_le_bytes());
        index.extend_from_slice(&MAGIC.to_le_bytes());
        self.file.write_all(&index)?;
        self.file.sync_all()?;
        invalidate_member_index(self.path.parent().unwrap_or_else(|| Path::new("")));
        Ok(())
    }
}

/// The v2 `index_len` for `count` members, or `None` when it does not fit the trailer's
/// u32: 4 bytes of count, 32 per entry, and the 64-byte extension.
fn v2_index_len(count: usize) -> Option<u32> {
    u64::try_from(count)
        .ok()?
        .checked_mul(INDEX_ENTRY_LEN)?
        .checked_add(4 + INDEX_V2_EXTENSION_LEN)
        .and_then(|len| u32::try_from(len).ok())
}

/// Read-only access. The limit is per member, NEVER per container. The generic
/// source supports byte-counted I/O without a separate implementation for tests.
pub struct ArchiveReader<R> {
    source: BufReader<R>,
    max_member_bytes: u64,
    // The number in the container's `fold-<stamp>.ark` file name, which a v2 index digest
    // includes. Without it a v2 index cannot be verified, and the reader behaves exactly as
    // it does for a v1 index.
    stamp: Option<u64>,
    index: Option<IndexOutcome>,
}

pub(crate) struct IndexEntry {
    address: [u8; 16],
    offset: u64,
    payload_len: u64,
}

/// What validating a sealed container's index established. I/O errors are never one of
/// these: they propagate, so a transient failure can never read as absence.
pub(crate) enum IndexOutcome {
    /// A v2 index whose geometry passes every v1 check and whose digest matches the bytes
    /// read. Its entries are every member, so a miss is absence.
    Verified {
        entries: Vec<IndexEntry>,
        // The membership cache keys a verified container by this digest, and searches the
        // entries on disk only when they are in address order. That cache is Unix-only.
        #[cfg_attr(not(unix), allow(dead_code))]
        digest: [u8; 32],
        // Whether the entries on disk are strictly ascending by address, which an on-disk
        // binary search needs. The writer always sorts them; nothing else depends on it.
        #[cfg_attr(not(unix), allow(dead_code))]
        sorted_on_disk: bool,
    },
    /// The entries pass the structural ("geometry") checks, their offsets tiling the member
    /// region with no repeated address, but nothing vouches that they are every member: a v1
    /// index, a v2 index whose digest, tag or reserved bytes do not match, or a v2 index
    /// read without a stamp. Entries are kept for the hit-path header check and the
    /// membership cross-check; a miss still needs the header scan.
    Unverified { entries: Vec<IndexEntry> },
    /// The entries fail the structural checks. Lookups recover by the header scan.
    Invalid(String),
}

impl IndexOutcome {
    fn invalid(message: &str) -> Self {
        Self::Invalid(message.to_owned())
    }
}

/// How the index answered one lookup.
#[derive(Debug)]
enum IndexLookup {
    Hit {
        offset: u64,
        payload_len: u64,
    },
    /// `authoritative` only when the index verified, so the miss means absence.
    Miss {
        authoritative: bool,
    },
}

impl ArchiveReader<File> {
    /// The number in a `fold-<stamp>.ark` file name is the stamp a v2 index digest includes.
    /// Any other name leaves a v2 index unverifiable, so it is read exactly as a v1 index is.
    pub fn open(path: impl AsRef<Path>, max_member_bytes: u64) -> io::Result<Self> {
        let path = path.as_ref();
        #[cfg(any(test, feature = "test-io-trace"))]
        expiry_io::opened();
        Ok(Self {
            source: BufReader::with_capacity(64 * 1024, File::open(path)?),
            max_member_bytes,
            stamp: fold_timestamp(path).ok(),
            index: None,
        })
    }
}

impl<R: Read + Seek> ArchiveReader<R> {
    /// The source must remain immutable while this reader is in use. Generic sources
    /// retain unbuffered I/O; `open` buffers files without changing its return type.
    pub fn new(source: R, max_member_bytes: u64) -> Self {
        Self {
            source: BufReader::with_capacity(0, source),
            max_member_bytes,
            stamp: None,
            index: None,
        }
    }

    /// Set the stamp of the container this source holds, the number in its
    /// `fold-<stamp>.ark` file name, so a v2 index can verify. It must be that number: a
    /// wrong stamp makes a valid v2 index fail verification and fall back to header scans,
    /// never the reverse.
    pub fn with_stamp(mut self, stamp: u64) -> Self {
        self.stamp = Some(stamp);
        self.index = None;
        self
    }

    fn trailer(&mut self) -> io::Result<(ContainerState, u64, u32)> {
        let size = self.source.seek(SeekFrom::End(0))?;
        if size < 8 {
            return Ok((ContainerState::Unsealed, size, 0));
        }
        self.source.seek(SeekFrom::End(-8))?;
        let mut bytes = [0; 8];
        self.source.read_exact(&mut bytes)?;
        let state = if u32::from_le_bytes(bytes[4..8].try_into().unwrap()) == MAGIC {
            ContainerState::Sealed
        } else {
            ContainerState::Unsealed
        };
        Ok((
            state,
            size - 8,
            u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        ))
    }

    pub fn state(&mut self) -> io::Result<ContainerState> {
        Ok(self.trailer()?.0)
    }

    fn sealed_trailer(&mut self) -> io::Result<(u64, u32)> {
        let (state, end, index_len) = self.trailer()?;
        if state != ContainerState::Sealed {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsealed archive is not readable",
            ));
        }
        Ok((end, index_len))
    }

    fn header(&mut self, offset: u64, end: u64) -> io::Result<MemberInfo> {
        header_in_bounds(offset, end)?;
        let mut bytes = [0; MEMBER_HEADER_LEN as usize];
        self.read_unbuffered(offset, &mut bytes)?;
        parse_header(&bytes, offset, end)
    }

    /// Read exactly `buf.len()` bytes at `offset`, straight from the underlying source.
    ///
    /// Header reads are 64 bytes at scattered offsets. Going through the 64 KiB
    /// `BufReader` would refill the whole buffer for each of them, because every seek
    /// throws the buffer away, so a header walk would read about 64 KiB per member
    /// instead of 64 bytes. Bypassing it is coherent: `BufReader::seek` always discards
    /// the buffer, so after the seek the buffer is empty and the underlying source's
    /// position is the reader's logical position, before and after this read.
    fn read_unbuffered(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.source.seek(SeekFrom::Start(offset))?;
        self.source.get_mut().read_exact(buf)
    }

    /// Walk headers without trusting index lengths or offsets. Payloads are skipped,
    /// not served or digest-verified. Even this metadata recovery refuses an unsealed
    /// file: its live sources still belong to the interrupted fold pass.
    pub fn rescan(&mut self) -> io::Result<Vec<MemberInfo>> {
        let (end, index_len) = self.sealed_trailer()?;
        self.scan(end, index_len)
    }

    fn scan(&mut self, end: u64, index_len: u32) -> io::Result<Vec<MemberInfo>> {
        let members_end = end
            .checked_sub(u64::from(index_len))
            .ok_or_else(|| invalid("invalid archive index length"))?;
        let mut entries = Vec::new();
        let mut seen = BTreeSet::new();
        let mut offset = 0;
        while offset < members_end {
            // The seal's index length identifies the boundary; damaged member magic
            // before it is never evidence that the member region ended early.
            let mut bytes = [0; MEMBER_HEADER_LEN as usize];
            header_in_bounds(offset, members_end)?;
            self.read_unbuffered(offset, &mut bytes)?;
            let info = parse_header(&bytes, offset, members_end)?;
            if !seen.insert(info.session_addr.clone()) {
                return Err(invalid("duplicate session in archive headers"));
            }
            offset += MEMBER_HEADER_LEN + info.payload_len;
            entries.push(info);
        }
        if index_len != 0 {
            let v1_len = 4 + INDEX_ENTRY_LEN * entries.len() as u64;
            if u64::from(index_len) != v1_len
                && u64::from(index_len) != v1_len + INDEX_V2_EXTENSION_LEN
            {
                return Err(invalid("archive scan cannot prove member coverage"));
            }
        }
        Ok(entries)
    }

    fn indexed(&mut self, addr: &str, end: u64, index_len: u32) -> io::Result<IndexLookup> {
        // A sealed container is immutable: the seal is the last write, seal(mut self)
        // consumes the writer, and create_new(true) prevents reopening its path through
        // ArchiveWriter. Thus a validated index cannot become invalid under this reader.
        // Validate lazily at the first lookup, not open/state/rescan, preserving when
        // corruption is refused and avoiding index work for metadata-only callers.
        if self.index.is_none() {
            self.index = Some(self.load_index(end, index_len)?);
        }
        let (entries, authoritative) = match self.index.as_ref().unwrap() {
            IndexOutcome::Verified { entries, .. } => (entries, true),
            IndexOutcome::Unverified { entries } => (entries, false),
            IndexOutcome::Invalid(message) => return Err(invalid(message)),
        };
        Ok(
            match entries.binary_search_by(|entry| entry.address.as_slice().cmp(addr.as_bytes())) {
                Ok(i) => IndexLookup::Hit {
                    offset: entries[i].offset,
                    payload_len: entries[i].payload_len,
                },
                Err(_) => IndexLookup::Miss { authoritative },
            },
        )
    }

    // Validate the complete index geometry, including contiguous coverage, before
    // publishing any cached entry. A malformed/missing entry must cost a rescan,
    // not hide a session. I/O errors are returned as errors and never cached, so a
    // transient failure can retry; only geometry failures become `Invalid`.
    pub(crate) fn load_index(&mut self, end: u64, index_len: u32) -> io::Result<IndexOutcome> {
        let len = u64::from(index_len);
        if len < 4 || len > end || (len - 4) % INDEX_ENTRY_LEN != 0 {
            return Ok(IndexOutcome::invalid("invalid archive index length"));
        }
        let start = end - len;
        self.source.seek(SeekFrom::Start(start))?;
        let mut count_bytes = [0; 4];
        self.source.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes);
        let entries_end = u64::from(count) * INDEX_ENTRY_LEN + 4;
        // The two layouts differ only by the extension, which is a multiple of the entry
        // length, so `count` alone decides which one the length describes.
        let v2 = if entries_end == len {
            false
        } else if entries_end + INDEX_V2_EXTENSION_LEN == len {
            true
        } else {
            return Ok(IndexOutcome::invalid("invalid archive index count"));
        };
        // Hash the bytes exactly as they are read, never a re-serialization of the parsed
        // or sorted entries: the digest vouches for what is on disk.
        let mut hasher = match self.stamp {
            Some(stamp) if v2 => {
                let mut hasher = Sha256::new();
                hasher.update(INDEX_V2_DOMAIN);
                hasher.update(stamp.to_le_bytes());
                hasher.update(index_len.to_le_bytes());
                hasher.update(count_bytes);
                Some(hasher)
            }
            _ => None,
        };
        let mut entries = Vec::new();
        let mut sorted_on_disk = true;
        for _ in 0..count {
            let mut entry = [0; INDEX_ENTRY_LEN as usize];
            self.source.read_exact(&mut entry)?;
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(entry);
            }
            let Ok(name) = std::str::from_utf8(&entry[..16]) else {
                return Ok(IndexOutcome::invalid("invalid index address"));
            };
            if let Err(error) = address(name) {
                return Ok(IndexOutcome::Invalid(error.to_string()));
            }
            let address: [u8; 16] = entry[..16].try_into().unwrap();
            if entries
                .last()
                .is_some_and(|previous: &IndexEntry| previous.address >= address)
            {
                sorted_on_disk = false;
            }
            entries.push(IndexEntry {
                address,
                offset: u64::from_le_bytes(entry[16..24].try_into().unwrap()),
                payload_len: u64::from_le_bytes(entry[24..32].try_into().unwrap()),
            });
        }
        // A v1 index lists members in offset order, and is checked in that order exactly
        // as it always was. A v2 index lists them by address, so its entries are put in
        // offset order for the same check.
        if v2 {
            entries.sort_unstable_by_key(|entry| entry.offset);
        }
        if let Err(message) = check_coverage(&entries, start) {
            return Ok(IndexOutcome::invalid(message));
        }
        entries.sort_unstable_by_key(|entry| entry.address);
        if !v2 {
            return Ok(IndexOutcome::Unverified { entries });
        }
        let mut extension = [0; INDEX_V2_EXTENSION_LEN as usize];
        self.source.read_exact(&mut extension)?;
        let stored: [u8; 32] = extension[..32].try_into().unwrap();
        let failure = match hasher {
            // Without the file-name stamp the digest cannot be checked. That leaves the index
            // unverifiable, which is not evidence of damage, so nothing is logged.
            None => None,
            Some(_) if extension[32..36] != INDEX_V2_TAG => Some("its tag is wrong"),
            Some(_) if extension[36..].iter().any(|byte| *byte != 0) => {
                Some("its reserved bytes are not zero")
            }
            Some(mut hasher) => {
                hasher.update(&extension[32..]);
                if <[u8; 32]>::from(hasher.finalize()) == stored {
                    return Ok(IndexOutcome::Verified {
                        entries,
                        digest: stored,
                        sorted_on_disk,
                    });
                }
                Some("its digest does not match")
            }
        };
        if let (Some(reason), Some(stamp)) = (failure, self.stamp) {
            warn_unverified_v2(stamp, &stored, reason);
        }
        Ok(IndexOutcome::Unverified { entries })
    }

    /// The members a verified v2 index lists, in offset order, or `None` when the index does
    /// not verify (a v1 index, a damaged one, or a reader without the container's stamp).
    /// Unlike [`rescan`](Self::rescan), which stops at the first member whose magic is
    /// damaged and silently omits every member after it, a verified index names them all.
    /// Nothing here checks the members themselves: read each one through `read_member`.
    pub fn verified_members(&mut self) -> io::Result<Option<Vec<IndexedMember>>> {
        let (end, index_len) = self.sealed_trailer()?;
        if self.index.is_none() {
            self.index = Some(self.load_index(end, index_len)?);
        }
        let Some(IndexOutcome::Verified { entries, .. }) = self.index.as_ref() else {
            return Ok(None);
        };
        let mut members = entries
            .iter()
            .map(|entry| IndexedMember {
                // `load_index` accepted only ASCII hex addresses, so this conversion is exact.
                session_addr: String::from_utf8_lossy(&entry.address).into_owned(),
                offset: entry.offset,
                payload_len: entry.payload_len,
            })
            .collect::<Vec<_>>();
        members.sort_unstable_by_key(|member| member.offset);
        Ok(Some(members))
    }

    /// Validates the index once per reader, then reads one member per hit. A miss in a
    /// verified v2 index is absence; a miss in any other index, or an index that fails
    /// validation, falls back to a header walk. The digest is always checked before
    /// returning bytes.
    pub fn read_member(&mut self, session_addr: &str) -> io::Result<Option<ArchiveMember>> {
        address(session_addr)?;
        let (end, index_len) = self.sealed_trailer()?;
        let info = match self.indexed(session_addr, end, index_len) {
            Ok(IndexLookup::Hit {
                offset,
                payload_len,
            }) => Some(self.located(
                session_addr,
                offset,
                payload_len,
                end - u64::from(index_len),
            )?),
            // A verified index lists every member, so nothing else can hold the address.
            Ok(IndexLookup::Miss {
                authoritative: true,
            }) => None,
            Ok(IndexLookup::Miss {
                authoritative: false,
            }) => self
                .scan(end, index_len)?
                .into_iter()
                .find(|entry| entry.session_addr == session_addr),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => self
                .scan(end, index_len)?
                .into_iter()
                .find(|entry| entry.session_addr == session_addr),
            Err(error) => return Err(error),
        };
        let Some(info) = info else { return Ok(None) };
        self.payload(info).map(Some)
    }

    /// Read the member an index entry locates, after checking its header names it.
    #[cfg(unix)]
    pub(crate) fn read_located(
        &mut self,
        session_addr: &str,
        offset: u64,
        payload_len: u64,
    ) -> io::Result<ArchiveMember> {
        let (end, index_len) = self.sealed_trailer()?;
        let members_end = end
            .checked_sub(u64::from(index_len))
            .ok_or_else(|| invalid("invalid archive index length"))?;
        let info = self.located(session_addr, offset, payload_len, members_end)?;
        self.payload(info)
    }

    fn located(
        &mut self,
        session_addr: &str,
        offset: u64,
        payload_len: u64,
        members_end: u64,
    ) -> io::Result<MemberInfo> {
        // A decoded index locates a member, so header failure is corruption, not a
        // reason to scan: scanning could mistake damaged member magic for the index
        // boundary and silently serve an older transcript.
        let info = self.header(offset, members_end)?;
        if info.session_addr != session_addr || info.payload_len != payload_len {
            return Err(invalid("archive index disagrees with header"));
        }
        Ok(info)
    }

    fn payload(&mut self, info: MemberInfo) -> io::Result<ArchiveMember> {
        #[cfg(any(test, feature = "test-io-trace"))]
        expiry_io::loaded();
        if info.payload_len > self.max_member_bytes {
            return Err(invalid("archive member exceeds per-member read limit"));
        }
        let len = usize::try_from(info.payload_len)
            .map_err(|_| invalid("archive member length exceeds address space"))?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(len)
            .map_err(|_| invalid("cannot allocate archive member"))?;
        payload.resize(len, 0);
        self.source
            .seek(SeekFrom::Start(info.offset + MEMBER_HEADER_LEN))?;
        self.source.read_exact(&mut payload)?;
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        if digest != info.payload_sha256 {
            return Err(invalid("archive member digest mismatch"));
        }
        Ok(ArchiveMember { info, payload })
    }
}

/// Log, once per container and stored digest in this process, that a v2 index failed to
/// verify. Unlike a v1 index, that is positive evidence of damage. The container is still
/// read exactly as a v1 container is, so nothing is lost but speed.
fn warn_unverified_v2(stamp: u64, stored: &[u8; 32], reason: &str) {
    static WARNED: std::sync::Mutex<BTreeSet<(u64, [u8; 32])>> =
        std::sync::Mutex::new(BTreeSet::new());
    if WARNED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((stamp, *stored))
    {
        tracing::warn!(
            target: "archive",
            "archive container fold-{stamp}.ark has a v2 index that does not verify ({reason}); misses in it are proven by a header scan, as for a v1 index"
        );
    }
}

/// Parse numeric precedence, not lexicographic order (fold-10 is newer than fold-9).
/// Noncanonical names are rejected so a timestamp has only one filename spelling.
pub fn fold_timestamp(path: impl AsRef<Path>) -> io::Result<u64> {
    let name = path
        .as_ref()
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid archive filename"))?;
    let stamp = name
        .strip_prefix("fold-")
        .and_then(|n| n.strip_suffix(".ark"))
        .ok_or_else(|| invalid("invalid archive filename"))?;
    let value: u64 = stamp
        .parse()
        .map_err(|_| invalid("invalid fold timestamp"))?;
    if stamp != value.to_string() {
        return Err(invalid("noncanonical fold timestamp"));
    }
    Ok(value)
}

/// Resolve among supplied containers without filesystem discovery or live-WAL routing.
/// Unsealed passes are ignored; corruption in a newer sealed member is an error, not
/// permission to silently return an older transcript prefix. Equal stamps are ambiguous.
/// Ordering by container start stamp is sound only because broca-core's
/// `lease::acquire_archive_pass` serializes mutating passes, and an unsealed resume
/// restarts with a fresh stamp rather than publishing new bytes under an old stamp.
pub fn read_newest_member(
    paths: &[PathBuf],
    session_addr: &str,
    max_member_bytes: u64,
) -> io::Result<Option<ArchiveMember>> {
    #[cfg(unix)]
    if let Some(answer) = member_index::lookup(paths, session_addr, max_member_bytes)? {
        return Ok(answer);
    }
    scan_newest_member(paths, session_addr, max_member_bytes)
}

fn scan_newest_member(
    paths: &[PathBuf],
    session_addr: &str,
    max_member_bytes: u64,
) -> io::Result<Option<ArchiveMember>> {
    let addr = address(session_addr)?;
    let mut ordered = paths
        .iter()
        .map(|path| Ok((fold_timestamp(path)?, path)))
        .collect::<io::Result<Vec<_>>>()?;
    ordered.sort_by_key(|(stamp, _)| std::cmp::Reverse(*stamp));
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid("ambiguous archive timestamps"));
    }
    #[cfg(unix)]
    membership::reap_missing();
    for (stamp, path) in ordered {
        // A CONTAINER THAT VANISHED BETWEEN DISCOVERY AND OPEN IS NOT A STORAGE FAILURE.
        // Callers snapshot PATHNAMES (`FileWalStore::archived_member` does a `read_dir`)
        // and reads are lease-free by design, so the archive-pass lease cannot exclude a
        // concurrent fold. Propagating NotFound here failed the WHOLE lookup: an
        // unrelated archived transcript returned a storage error, and a genuinely NEW
        // session could fail admission while establishing it has no archived predecessor.
        //
        // Pass retirement takes unsealed containers, which the next line skips
        // anyway; unfold restores its members to the live path first, where
        // live-wins finds them. Neither leaves needed members in the vanished file.
        //
        // ONLY NotFound. Any other error still fails loud -- a permission or I/O fault is
        // a real storage failure and must not be laundered into "no member here".
        let mut reader = match ArchiveReader::open(path, max_member_bytes) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if reader.state()? == ContainerState::Unsealed {
            continue;
        }
        #[cfg(unix)]
        match membership::lookup(path, stamp, &mut reader, &addr)? {
            membership::Answer::Absent => continue,
            // A verified index located it, so this is the newest container holding it.
            membership::Answer::Located {
                offset,
                payload_len,
            } => {
                return reader
                    .read_located(session_addr, offset, payload_len)
                    .map(Some)
            }
            membership::Answer::Unknown => {}
        }
        // Without a stable file identity there is no cache, but a verified index still
        // answers a miss inside `read_member` without a header scan.
        #[cfg(not(unix))]
        let _ = (stamp, addr);
        if let Some(member) = reader.read_member(session_addr)? {
            return Ok(Some(member));
        }
    }
    Ok(None)
}

/// Warm the per-container fallback cache without replacing a directory-wide index with
/// a single-container snapshot. The caller may warm independent containers concurrently.
pub fn prewarm_container_membership(path: &Path, max_member_bytes: u64) -> io::Result<()> {
    scan_newest_member(
        std::slice::from_ref(&path.to_owned()),
        "0000000000000000",
        max_member_bytes,
    )?;
    Ok(())
}

/// Discard cached locations in an archive directory after a fold, removal or restore.
/// External changes are also detected by comparing file identities on every lookup.
pub fn invalidate_member_index(directory: &Path) {
    #[cfg(unix)]
    member_index::invalidate(directory);
    #[cfg(not(unix))]
    let _ = directory;
}

/// Mark a restored address uncertain without discarding other unchanged locations.
/// Archive reads of this address use the scan until the directory index is rebuilt;
/// ordinary store reads prefer the restored live log before consulting the archive.
pub fn invalidate_restored_member(directory: &Path, session_addr: &str) {
    #[cfg(unix)]
    member_index::restored(directory, session_addr);
    #[cfg(not(unix))]
    let _ = (directory, session_addr);
}

#[cfg(unix)]
mod member_index;

/// Diagnostic (container count, address count, estimated allocation bytes) for a directory.
#[cfg(all(
    unix,
    any(test, feature = "test-io-trace", feature = "test-known-feature")
))]
#[doc(hidden)]
pub fn cached_member_index_stats(directory: &Path) -> Option<(usize, usize, usize)> {
    member_index::stats(directory)
}

/// How the process-wide membership cache answered for one container, for tests and
/// diagnostics in other crates. Not an API for deciding anything: lookups never need it.
#[cfg(unix)]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachedMembership {
    /// Every member header matched the index, so the index's addresses are the members.
    FromIndex,
    /// Built by walking every member header and cross-checking the index against them.
    FromHeaders,
    /// Built and refused: lookups in this container stay uncached and authoritative.
    Disabled,
    /// Built, but the address budget had no room: lookups stay uncached and authoritative.
    OverBudget,
    /// The container's v2 index verified: its index is every member, so no member header
    /// was read. Its addresses may or may not be resident; either way a miss reads no
    /// member bytes.
    Verified,
}

/// The cached membership of the container at `path`, or `None` if no build has finished
/// for it in this process. Read-only: it never starts a build or takes a cache slot.
#[cfg(unix)]
#[doc(hidden)]
pub fn cached_membership(path: &Path) -> Option<CachedMembership> {
    membership::cached(path)
}

#[cfg(unix)]
mod membership;

/// Thread-local archive I/O accounting for synchronous expiry regression tests.
#[cfg(any(test, feature = "test-io-trace"))]
#[doc(hidden)]
pub mod expiry_io {
    use std::cell::Cell;

    thread_local! {
        static COUNTS: Cell<(u64, u64, u64)> = const { Cell::new((0, 0, 0)) };
    }

    pub(super) fn opened() {
        COUNTS.with(|counts| {
            let (opens, loads, decodes) = counts.get();
            counts.set((opens + 1, loads, decodes));
        });
    }

    pub(super) fn loaded() {
        COUNTS.with(|counts| {
            let (opens, loads, decodes) = counts.get();
            counts.set((opens, loads + 1, decodes));
        });
    }

    pub(crate) fn decoded() {
        COUNTS.with(|counts| {
            let (opens, loads, decodes) = counts.get();
            counts.set((opens, loads, decodes + 1));
        });
    }

    /// Returns (archive open attempts, payload-load attempts, WAL-decode attempts).
    /// Each count advances before the operation on the calling thread.
    pub fn counts() -> (u64, u64, u64) {
        COUNTS.with(Cell::get)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cache_tests;

#[cfg(test)]
mod fixture_tests;

#[cfg(test)]
mod v2_tests;

#[cfg(test)]
mod writer_tests;
