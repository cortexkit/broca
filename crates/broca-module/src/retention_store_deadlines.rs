//! The deadlines retention found for the sessions in the session store's ephemeral root, kept on
//! disk between restarts.
//!
//! A pass reads a session's records again only once its stored position has moved since its
//! deadline was found (see `StoreRetention::deadlines`). Held only in memory, that cache was
//! empty after every restart, so the first pass read the open log of every ephemeral session:
//! 18,746 sessions and 4.7 GB at one restart. A position names the head version, the open log
//! and its length, so a session written to since moves and is read again.
//!
//! The file is a cache. One that is missing, unreadable, or of another format is ignored, and
//! the pass reads the sessions as it did before the file existed.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::Path;

use broca_session_store::codec::Id128;
use broca_session_store::session::Position;

const FILE: &str = "retention-deadlines.json";
const FORMAT: u32 = 1;

/// Each session's deadline, as its records said at the stored position it was read at, by
/// address; `None` when the records set none.
pub(super) type Deadlines = HashMap<String, (Position, Option<u64>)>;

#[derive(serde::Serialize, serde::Deserialize)]
struct Stored {
    format: u32,
    /// Address to head version, open log id (hex), log length read, and deadline.
    sessions: BTreeMap<String, (u64, String, u64, Option<u64>)>,
}

fn hex(id: &Id128) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Option<Id128> {
    if text.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (i, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(Id128(id))
}

/// The deadlines kept in the ephemeral root `root`, or none when there is no usable file.
pub(super) fn load(root: &Path) -> Deadlines {
    let Ok(bytes) = std::fs::read(root.join(FILE)) else {
        return Deadlines::new();
    };
    let Ok(stored) = serde_json::from_slice::<Stored>(&bytes) else {
        tracing::info!(target: "retention", "the kept retention deadlines don't decode; every session is read again");
        return Deadlines::new();
    };
    if stored.format != FORMAT {
        return Deadlines::new();
    }
    stored
        .sessions
        .into_iter()
        .filter_map(|(address, (version, log, len, deadline))| {
            let log = unhex(&log)?;
            Some((address, (Position { version, log, len }, deadline)))
        })
        .collect()
}

/// Replaces the kept deadlines in `root` with `deadlines`: written beside the file, then
/// renamed over it, so a reader finds the old file or the new one whole.
pub(super) fn save(root: &Path, deadlines: &Deadlines) -> std::io::Result<()> {
    let stored = Stored {
        format: FORMAT,
        sessions: deadlines
            .iter()
            .map(|(address, (position, deadline))| {
                (
                    address.clone(),
                    (
                        position.version,
                        hex(&position.log),
                        position.len,
                        *deadline,
                    ),
                )
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
    let temporary = root.join(format!("{FILE}.tmp"));
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    drop(file);
    std::fs::rename(&temporary, root.join(FILE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kept_deadlines_read_back_and_a_bad_file_reads_as_none() {
        let root = broca_test_support::scratch_root()
            .join(format!("retention-deadlines-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let mut deadlines = Deadlines::new();
        let position = Position {
            version: 7,
            log: Id128([0xab; 16]),
            len: 4096,
        };
        deadlines.insert("a".into(), (position, Some(1_800_000)));
        deadlines.insert("b".into(), (position, None));
        save(&root, &deadlines).unwrap();
        assert_eq!(load(&root), deadlines);
        std::fs::write(root.join(FILE), b"{not json").unwrap();
        assert!(load(&root).is_empty());
        std::fs::remove_dir_all(&root).unwrap();
    }
}
