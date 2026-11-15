//! A session's enrollment in the current compaction contract: what `session.entries` serves about
//! the messages placed before it.
//!
//! A session that ran outside the contract keeps every record it wrote. When a run moves it onto
//! the contract, that run decides once, for each earlier message, the part number before its first
//! numbered part (in the projection's numbering), its time, whether it is a global system message
//! the run's own system text takes the place of, and who delivered it where the records show a
//! module did. Those decisions are a session object, stored as an import's id map is: written
//! under the open log and made durable before the batch whose `CompactionEnrolled` names it, then
//! read once and kept while the session is open. The object is never deleted, as the import's
//! isn't: the operational fold names it for the session's life, and every checkpoint carries that.
//! A missing or unreadable object is damage, refused by name, never an empty overlay.
//!
//! The decisions overlay the entries the session serves to compaction ([`Session::compaction_entries`],
//! and the window's through [`EnrollmentOverlay::apply`]). The records and their own facts are
//! never rewritten, so `session.read` serves every earlier message as it did before.

use std::sync::Arc;

use super::{Session, SessionError, StoredEntry};
use crate::backend::SessionBackend;
use crate::codec::{hex, Message as _};
use crate::record::{EnrolledEntries, EnrolledEntry, EnrollmentOverlayRef};

/// A session's enrollment decisions, once read, with the object they were read from.
pub(crate) type OverlayCache =
    std::sync::Mutex<Option<(EnrollmentOverlayRef, Arc<EnrolledEntries>)>>;

/// What an enrolled session serves about the messages placed before it enrolled.
#[derive(Debug, Clone, PartialEq)]
pub struct EnrollmentOverlay {
    entries: Arc<EnrolledEntries>,
}

impl EnrollmentOverlay {
    /// The overlay over `entries`, which must hold one entry per ordinal from 1, in order.
    pub fn new(entries: Arc<EnrolledEntries>) -> Result<Self, String> {
        if let Some((at, entry)) = entries
            .entries
            .iter()
            .enumerate()
            .find(|(at, entry)| entry.ordinal != *at as u64 + 1)
        {
            return Err(format!(
                "its entry at position {at} names ordinal {}",
                entry.ordinal
            ));
        }
        Ok(Self { entries })
    }

    /// The newest message placed before the session enrolled.
    pub fn through_ordinal(&self) -> u64 {
        self.entries.entries.len() as u64
    }

    /// The decision about message `ordinal`, if it was placed before the session enrolled.
    pub fn entry(&self, ordinal: u64) -> Option<&EnrolledEntry> {
        let at = usize::try_from(ordinal.checked_sub(1)?).ok()?;
        self.entries.entries.get(at)
    }

    /// Whether message `ordinal` is a global system message the enrollment suppressed.
    pub fn suppressed(&self, ordinal: u64) -> bool {
        self.entry(ordinal).is_some_and(|entry| entry.suppressed)
    }

    /// `stored` as `session.entries` serves it on the contract: an earlier message takes the
    /// enrollment's part number base, time and notice in place of its own facts' (recorded off
    /// the contract, where nothing served them). A message placed since is unchanged.
    ///
    /// A suppressed global system message is still served, at its own ordinal and under its own
    /// id, but as a system message with no content, so its entry has no parts. The contract
    /// counts every message of a host that keeps its own log, so ordinals have no gaps; the
    /// run's system text took the message's place above the conversation, so its entry shows
    /// nothing, and a view's render plan renders an entry with no parts as nothing at all. With
    /// no parts and no notice, the entry's digest is the same at every read.
    pub fn apply(&self, mut stored: StoredEntry) -> StoredEntry {
        let Some(decided) = self.entry(stored.ordinal) else {
            return stored;
        };
        // The served facts are this read's own copy: the conversation's stay as recorded.
        if let Some(facts) = stored.state.facts.as_mut().map(std::sync::Arc::make_mut) {
            facts.tag_base = decided.tag_base;
            facts.written_at_ms = Some(decided.written_at_ms);
            facts.notice = decided.notice.clone().map(Into::into);
            if decided.suppressed {
                // Nothing that could give the empty entry a notice origin, a speaker or usage.
                facts.sender = None;
                facts.marks = None;
                facts.usage = None;
                facts.generated_under = None;
            }
        }
        if decided.suppressed {
            std::sync::Arc::make_mut(&mut stored.message)
                .content
                .clear();
            stored.next = None;
            stored.state.delivered.clear();
            stored.state.turn = None;
        }
        stored
    }

    /// `entries` as `session.entries` serves them (see [`EnrollmentOverlay::apply`]); `overlay`
    /// is `None` for a session that never enrolled, which serves them as stored.
    pub fn serve(overlay: Option<&Self>, entries: Vec<StoredEntry>) -> Vec<StoredEntry> {
        match overlay {
            Some(overlay) => entries
                .into_iter()
                .map(|entry| overlay.apply(entry))
                .collect(),
            None => entries,
        }
    }
}

/// The name of the enrollment object `overlay` points at.
pub(crate) fn enrollment_name(overlay: &EnrollmentOverlayRef) -> String {
    format!(
        "{}/a/{}.{}-compaction-enrollment",
        hex(&overlay.log.0),
        &hex(&overlay.generation.0)[..16],
        overlay.seq,
    )
}

impl<B: SessionBackend> Session<B> {
    /// The name of the enrollment object `overlay` points at.
    fn enrollment_name(&self, overlay: &EnrollmentOverlayRef) -> String {
        enrollment_name(overlay)
    }

    /// Stores an enrollment's decisions, `entries`, as a session object under the open log,
    /// durable before this returns, and names it for the batch about to be appended: the
    /// enrolling run's start batch, whose `CompactionEnrolled` record must name what this
    /// returns. A batch that never commits leaves an object nothing names.
    pub async fn put_enrollment(
        &mut self,
        entries: &EnrolledEntries,
    ) -> Result<EnrollmentOverlayRef, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let entries = Arc::new(entries.clone());
        EnrollmentOverlay::new(entries.clone()).map_err(SessionError::EnrollmentDamaged)?;
        let overlay = EnrollmentOverlayRef {
            log: self.log,
            generation: self.head.generation,
            seq: self.next_seq(),
        };
        let name = self.enrollment_name(&overlay);
        let sealed = self
            .key
            .frame_key()
            .seal_unit(name.as_bytes(), &entries.encode());
        self.backend
            .put_object(&self.addr, &name, &sealed, self.version)
            .await?;
        self.backend
            .sync_objects(&self.addr, &self.log, self.version)
            .await?;
        *self.enrollment.lock().expect("enrollment cache") = Some((overlay.clone(), entries));
        Ok(overlay)
    }

    /// The decisions of the session's enrollment in the current compaction contract, or `None`
    /// for a session that never enrolled. Read once, then kept. An object the session names but
    /// can't read, or whose entries don't cover exactly the messages before the enrollment, is
    /// damage.
    pub async fn enrollment(&self) -> Result<Option<EnrollmentOverlay>, SessionError> {
        let Some(enrolled) = self.operations.enrolled.as_ref() else {
            return Ok(None);
        };
        let cached = self
            .enrollment
            .lock()
            .expect("enrollment cache")
            .as_ref()
            .filter(|(overlay, _)| *overlay == enrolled.overlay)
            .map(|(_, entries)| entries.clone());
        let entries = match cached {
            Some(entries) => entries,
            None => {
                self.storage_read()?;
                let name = self.enrollment_name(&enrolled.overlay);
                let damaged =
                    |reason: &str| SessionError::EnrollmentDamaged(format!("{name}: {reason}"));
                let sealed = self
                    .backend
                    .get_object(&self.addr, &name)
                    .await?
                    .ok_or_else(|| damaged("missing"))?;
                let bytes = self
                    .key
                    .frame_key()
                    .open_unit(name.as_bytes(), &sealed)
                    .ok_or_else(|| damaged("doesn't open under the session key"))?;
                let entries = Arc::new(
                    EnrolledEntries::decode(&bytes)
                        .map_err(|error| damaged(&format!("doesn't decode: {error}")))?,
                );
                *self.enrollment.lock().expect("enrollment cache") =
                    Some((enrolled.overlay.clone(), entries.clone()));
                entries
            }
        };
        let name = || self.enrollment_name(&enrolled.overlay);
        let overlay = EnrollmentOverlay::new(entries)
            .map_err(|error| SessionError::EnrollmentDamaged(format!("{}: {error}", name())))?;
        if overlay.through_ordinal() != enrolled.through_ordinal {
            return Err(SessionError::EnrollmentDamaged(format!(
                "{}: it holds {} entries, but the session enrolled after message {}",
                name(),
                overlay.through_ordinal(),
                enrolled.through_ordinal
            )));
        }
        Ok(Some(overlay))
    }

    /// Up to `limit` entries from ordinal `from` on, as `session.entries` serves them: as
    /// [`Session::entries`] reads them, with what the session's enrollment in the current
    /// compaction contract decided about the messages placed before it (see
    /// [`EnrollmentOverlay::apply`]). Every message has an entry, a suppressed one included, so
    /// the ordinals have no gaps. Every reader that projects entries for compaction reads them
    /// here, so they all see the same entries; `session.read` reads [`Session::entries`].
    pub async fn compaction_entries(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let overlay = self.enrollment().await?;
        let entries = self.entries(from, limit).await?;
        Ok(EnrollmentOverlay::serve(overlay.as_ref(), entries))
    }

    /// The entries whose message ids `mids` lists, found by their ids
    /// ([`Session::entries_by_mid`]) and served as [`Session::compaction_entries`] serves them.
    pub async fn compaction_entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let overlay = self.enrollment().await?;
        let entries = self.entries_by_mid(mids).await?;
        Ok(EnrollmentOverlay::serve(overlay.as_ref(), entries))
    }
}

#[cfg(test)]
mod tests;
