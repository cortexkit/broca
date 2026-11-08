//! The session head: the one small, mutable object of a session.
//!
//! The head holds pointers, not state: which log is open, the session's identities, its wrapped
//! data key, the writer generation and next seq, and the roots of the session's directories. Its
//! size never grows with history. Every
//! change to a session commits by one compare-and-swap of the head, so the head is what decides
//! which log is the session's.
//!
//! The head is stored readable, so a reader can find the session's objects. An encrypted
//! session's head is authenticated with its data key: a head changed by anyone without the key
//! fails to open. A plain session's head carries the unkeyed check its units carry instead (see
//! `frame::FrameKey::seal_unit`), which catches damage but not a deliberate edit.
//!
//! The head's mode is its `cipher` field, absent for an encrypted head. Its catalog fields are
//! readable without a key, so the opener reads the mode first and then checks the head in that
//! mode only: a head failing its check in the mode it names is damage, never read in the other
//! mode.

use crate::codec::{ByteBuf, DecodeError, Id128, Message as _};
use crate::keys::{self, Cipher, DataKey, DeploymentKey, UnwrapError};

/// The head format this build writes and reads. A head of any other format refuses by name.
pub const HEAD_FORMAT: u32 = 1;

const HEAD_CONTEXT: &[u8] = b"broca-session-head-v1";
/// The bytes before the head's body: an encrypted head's nonce and tag, or a plain head's
/// [`PLAIN_PAD`] zero bytes and its check. Both modes keep one length, so the body is found
/// before the mode is known.
const TAG_LEN: usize = 24 + 16;
/// The zero bytes a plain head's prefix starts with, where an encrypted head has its nonce.
const PLAIN_PAD: usize = TAG_LEN - crate::frame::CHECK_LEN;

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
        /// A recovery (see `session::recover`) began and hasn't committed; its change is named.
        /// Only a recovery opens the session in this state: it deletes what that change created
        /// and recovers again from the unchanged damaged log.
        3 Recovering(Id128),
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
        /// Set by the cold step (see `session::cold`) and cleared when a writer opens the session
        /// again, so a catalog rebuilt from heads knows which sessions are cold.
        17 opt cold_since_ms: u64,
        /// The setting the session's first log was created with (see `Conversation::new`).
        /// Absent means false. A damaged checkpoint of that first log is rebuilt from it.
        18 opt appends_in_conversation: bool,
        /// The session's cipher mode, set when it was created and never changed. Absent means
        /// encrypted: only a plain head writes it, so an encrypted head's bytes are as before
        /// plain mode existed, and a build that doesn't know plain mode refuses a plain head.
        19 opt [must] cipher: Cipher,
    }
}

impl Head {
    /// The session's cipher mode, as its head names it.
    pub fn cipher(&self) -> Cipher {
        self.cipher.unwrap_or(Cipher::XChaCha20Poly1305)
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
    /// The head is encrypted and the deployment key hasn't loaded.
    NotLoaded,
    Unwrap(UnwrapError),
    /// The head fails its check in the mode it names: an encrypted head's tag doesn't match its
    /// data key, or a plain head's check doesn't match its bytes, or a head is shaped unlike
    /// any its mode writes. Damage, or a change by someone without the key.
    Authentication,
}

/// The stored form of `head`: the bytes that authenticate it in its mode, then its encoded body.
/// `key` must be in the mode the head names.
pub fn seal(head: &Head, key: &DataKey) -> Vec<u8> {
    debug_assert_eq!(
        head.cipher(),
        key.cipher(),
        "a head is sealed in its own mode"
    );
    let body = head.encode();
    let mut out = match key.cipher() {
        Cipher::XChaCha20Poly1305 => keys::authenticate(key, HEAD_CONTEXT, &body),
        Cipher::None => {
            let mut prefix = vec![0u8; PLAIN_PAD];
            prefix.extend_from_slice(&crate::frame::plain_check(HEAD_CONTEXT, &body));
            prefix
        }
    };
    out.extend_from_slice(&body);
    out
}

/// The cipher mode a stored head names, read without checking it. Only for choosing how to open
/// the session: [`open`] then checks the head in that mode, and refuses it when it fails.
pub fn stored_cipher(bytes: &[u8]) -> Result<Cipher, HeadError> {
    let body = bytes.get(TAG_LEN..).ok_or(HeadError::Malformed)?;
    Ok(Head::decode(body).map_err(HeadError::Decode)?.cipher())
}

/// Opens a stored head: decodes it, then checks it in the mode it names. An encrypted head's data
/// key is unwrapped under `deployment` and checks its tag; a plain head checks its unkeyed check
/// and needs no deployment key.
pub fn open(bytes: &[u8], deployment: &DeploymentKey) -> Result<(Head, DataKey), HeadError> {
    if bytes.len() < TAG_LEN {
        return Err(HeadError::Malformed);
    }
    let (tag, body) = bytes.split_at(TAG_LEN);
    let head = Head::decode(body).map_err(HeadError::Decode)?;
    if head.format != HEAD_FORMAT {
        return Err(HeadError::Format(head.format));
    }
    match head.cipher() {
        Cipher::None => {
            let (pad, check) = tag.split_at(PLAIN_PAD);
            let checks = pad.iter().all(|byte| *byte == 0)
                && check == crate::frame::plain_check(HEAD_CONTEXT, body);
            // A plain session has no data key, so its head wraps none.
            if !checks || !head.keys.is_empty() {
                return Err(HeadError::Authentication);
            }
            Ok((head, DataKey::plain()))
        }
        Cipher::XChaCha20Poly1305 => {
            // Every encrypted head wraps its data key; one that wraps none was never written so.
            if head.keys.is_empty() {
                return Err(HeadError::Authentication);
            }
            if !deployment.is_loaded() {
                return Err(HeadError::NotLoaded);
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
    }
}

#[cfg(test)]
mod tests;
