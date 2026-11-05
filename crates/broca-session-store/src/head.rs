//! The session head: the one small, mutable object of a session.
//!
//! The head holds pointers, not state: which log is open, the session's identities, its wrapped
//! data key, the writer generation and next seq, and the roots of the session's directories. Its
//! size never grows with history. Every
//! change to a session commits by one compare-and-swap of the head, so the head is what decides
//! which log is the session's.
//!
//! The head is stored readable, so a reader can find the session's objects, but authenticated
//! with the session's data key: a head changed by anyone without the key fails to open.

use crate::codec::{ByteBuf, DecodeError, Id128, Message as _};
use crate::keys::{self, DataKey, DeploymentKey, UnwrapError};

/// The head format this build writes and reads. A head of any other format refuses by name.
pub const HEAD_FORMAT: u32 = 1;

const HEAD_CONTEXT: &[u8] = b"broca-session-head-v1";
const TAG_LEN: usize = 24 + 16;

crate::message! {
    /// The (project root, harness, session) a session was created under. The head keeps it, so
    /// a load whose triple differs refuses, and a hash collision of addresses can never open
    /// another session's data.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SessionTriple {
        1 req project_root: String,
        2 req harness: String,
        3 req session: String,
    }
}

crate::message! {
    /// The session's data key, wrapped under a deployment key.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WrappedKey {
        /// The data key's id, which frames name.
        1 req id: u16,
        /// The id of the deployment key it is wrapped under.
        2 req deployment: u16,
        3 req wrapped: ByteBuf,
    }
}

crate::message! {
    /// The session's open log.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OpenLog {
        /// The id of the change that created the log, which names it.
        1 req change: Id128,
        /// The length synced when the log was created. It counts toward the log's durable bound.
        2 req synced_len: u64,
    }
}

crate::message! {
    /// The last change the head committed, and the seq of its intent in the log it replaced.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LastChange {
        1 req change: Id128,
        2 req intent_seq: u64,
    }
}

crate::message! {
    /// What a committed change stopped naming: the log it replaced and the directory pages it
    /// rewrote. They're deleted once `unreferenced_at_ms` is a grace period old, so a reader that
    /// read the head just before the commit can finish reading them first.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PendingDeletion {
        1 req change: Id128,
        2 req unreferenced_at_ms: u64,
        3 rep logs: Id128,
        4 rep objects: String,
        /// An id-index list of retired runs (see `idindex::Retired`): their data pages and ref
        /// pages are deleted with the list, so the head doesn't carry every page's name.
        5 opt retired_index: String,
        /// A revert's list of the sealed segments and directory pages its branch stopped
        /// naming (an `idindex::Retired` object), deleted with the list.
        6 opt abandoned: String,
    }
}

crate::oneof! {
    /// Where the session stands. A head in any state but `Local` names an operation that
    /// crashed before its commit, which the next load finishes.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Residence {
        1 Local(()),
        /// Creating the session crashed before its commit; the log it was creating is named.
        2 Creating(Id128),
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Head {
        1 req format: u32,
        /// Random, minted when the session is created; it never changes.
        2 req session_id: Id128,
        3 req triple: SessionTriple,
        /// Random, minted when the session is created and again at every restore.
        4 req incarnation: Id128,
        5 req history_version: u64,
        6 req residence: Residence,
        /// The must-understand record kinds this session has ever written.
        7 rep features: String,
        8 rep keys: WrappedKey,
        /// The current writer's generation.
        9 req generation: Id128,
        /// The seq the next record takes, as of the last head update. A head update never
        /// moves it backwards.
        10 req next_seq: u64,
        11 opt open_log: OpenLog,
        12 opt last_change: LastChange,
        /// The roots of the transcript and ledger directories (see `directory`).
        13 opt transcript_dir: crate::directory::PageRef,
        14 opt ledger_dir: crate::directory::PageRef,
        /// Deletions waiting out the grace period. Every change runs due deletions before it
        /// commits, so this holds only the changes of the last grace period.
        15 rep pending: PendingDeletion,
        /// The id index's root object (see `idindex`). Absent until the first seal.
        16 opt id_index: String,
    }
}

/// Why a stored head couldn't be opened.
#[derive(Debug)]
pub enum HeadError {
    Malformed,
    Decode(DecodeError),
    /// A head written by a newer format.
    Format(u32),
    /// The head names no data key wrapped under the deployment key given.
    NoKey,
    Unwrap(UnwrapError),
    /// The head's tag doesn't match: it was changed by someone without the key.
    Authentication,
}

/// The stored form of `head`: its tag under the data key, then its encoded body.
pub fn seal(head: &Head, key: &DataKey) -> Vec<u8> {
    let body = head.encode();
    let mut out = keys::authenticate(key, HEAD_CONTEXT, &body);
    out.extend_from_slice(&body);
    out
}

/// Opens a stored head: decodes it, unwraps its data key under `deployment`, and checks its tag.
pub fn open(bytes: &[u8], deployment: &DeploymentKey) -> Result<(Head, DataKey), HeadError> {
    if bytes.len() < TAG_LEN {
        return Err(HeadError::Malformed);
    }
    let (tag, body) = bytes.split_at(TAG_LEN);
    let head = Head::decode(body).map_err(HeadError::Decode)?;
    if head.format != HEAD_FORMAT {
        return Err(HeadError::Format(head.format));
    }
    let wrapped = head
        .keys
        .iter()
        .find(|key| key.deployment == deployment.id())
        .ok_or(HeadError::NoKey)?;
    let key = keys::unwrap(deployment, &head.session_id, wrapped.id, &wrapped.wrapped.0)
        .map_err(HeadError::Unwrap)?;
    if !keys::verify(&key, HEAD_CONTEXT, body, tag) {
        return Err(HeadError::Authentication);
    }
    Ok((head, key))
}

#[cfg(test)]
mod tests;
