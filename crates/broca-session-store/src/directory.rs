//! Directories: which sealed segments a session's history is in.
//!
//! There is one directory per stream. Each entry describes one sealed segment, and entries are
//! kept in seq order in a small tree of immutable pages, `FANOUT` entries or children each. The
//! head names only the root, so it stays the same size however many segments a session has.
//!
//! Segments are only ever added at the end, so adding one rewrites only the tree's rightmost
//! path: the last leaf and its ancestors, at most one page per level. Every rewritten page is a new
//! object under the change that adds the segment, and the pages it replaces are retired with that
//! change. A lookup by seq walks from the root to one leaf.
//!
//! A revert cuts the branch at a seq: `truncate` keeps every segment wholly before it, gives the
//! segment holding it an entry whose `last_seq` ends just before it, and drops the rest. Pages
//! wholly before the cut are shared, and only the path to the cut is rewritten. An entry's
//! `last_seq` can therefore end before its segment's last record, and every reader of a segment
//! ignores records above it (`DirEntry::keeps`).
//!
//! Pages are encrypted under the session's frame key, with the session id and the page's own name
//! in their authenticated data, so a page can't be served under another name or session.

use crate::backend::{BackendError, SessionAddr, SessionBackend, Version};
use crate::codec::{Id128, Message as _};
use crate::frame::FrameKey;

/// Entries or children per page.
pub const FANOUT: usize = 256;
const PAGE_CONTEXT: &[u8] = b"broca-directory-v1";

crate::message! {
    /// One sealed segment.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DirEntry {
        /// The segment's object name.
        1 req object: String,
        2 req first_seq: u64,
        3 req last_seq: u64,
        /// The segment object's size in bytes.
        4 req size: u64,
        5 req sealed_at_ms: u64,
        /// In a transcript directory: the ordinals of the first and last messages the segment
        /// places. Absent when it places none.
        6 opt first_ordinal: u64,
        7 opt last_ordinal: u64,
    }
}

impl DirEntry {
    /// Whether this branch keeps the segment's record at `seq`: a revert can end an entry
    /// before its segment does.
    pub fn keeps(&self, seq: u64) -> bool {
        self.first_seq <= seq && seq <= self.last_seq
    }
}

crate::message! {
    /// A page, as its parent or the head names it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PageRef {
        1 req object: String,
        /// The page's height: 0 for a leaf, which holds entries.
        2 req level: u32,
        3 req first_seq: u64,
        4 req last_seq: u64,
        /// Entries or children the page holds.
        5 req count: u32,
        /// The ordinal range of the messages the page's segments place, as in [`DirEntry`].
        6 opt first_ordinal: u64,
        7 opt last_ordinal: u64,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Page {
        1 req level: u32,
        /// A leaf's entries.
        2 rep entries: DirEntry,
        /// An inner page's children.
        3 rep children: PageRef,
    }
}

#[derive(Debug)]
pub enum DirectoryError {
    Backend(BackendError),
    /// A page the tree names is missing.
    Missing(String),
    /// A page failed to authenticate or decode.
    Corrupt(String),
    /// A new entry doesn't follow the last one in seq order.
    OutOfOrder,
}

impl From<BackendError> for DirectoryError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

fn aad(session: &Id128, name: &str) -> Vec<u8> {
    let mut aad = PAGE_CONTEXT.to_vec();
    aad.extend_from_slice(&session.0);
    aad.extend_from_slice(&(name.len() as u32).to_le_bytes());
    aad.extend_from_slice(name.as_bytes());
    aad
}

fn page_ref(object: String, page: &Page) -> PageRef {
    // Ordinals increase with seq, so a page's range runs from its first item that places a
    // message to its last one.
    let (first_seq, last_seq, count, first_ordinal, last_ordinal) = if page.level == 0 {
        (
            page.entries.first().map_or(0, |entry| entry.first_seq),
            page.entries.last().map_or(0, |entry| entry.last_seq),
            page.entries.len(),
            page.entries.iter().find_map(|entry| entry.first_ordinal),
            page.entries
                .iter()
                .rev()
                .find_map(|entry| entry.last_ordinal),
        )
    } else {
        (
            page.children.first().map_or(0, |child| child.first_seq),
            page.children.last().map_or(0, |child| child.last_seq),
            page.children.len(),
            page.children.iter().find_map(|child| child.first_ordinal),
            page.children
                .iter()
                .rev()
                .find_map(|child| child.last_ordinal),
        )
    };
    PageRef {
        object,
        level: page.level,
        first_seq,
        last_seq,
        count: count as u32,
        first_ordinal,
        last_ordinal,
    }
}

/// Whether an item whose messages span `first..=last` holds `ordinal`.
fn holds_ordinal(first: Option<u64>, last: Option<u64>, ordinal: u64) -> bool {
    first
        .zip(last)
        .is_some_and(|(first, last)| first <= ordinal && ordinal <= last)
}

/// Reads and writes one session's directory pages.
pub struct Pages<'a, B: SessionBackend> {
    pub backend: &'a B,
    pub addr: &'a SessionAddr,
    pub session: Id128,
    pub key: &'a FrameKey,
    /// Entries or children per page: [`FANOUT`], smaller only in tests.
    pub fanout: usize,
}

/// What appending to a directory produced.
pub struct Appended {
    pub root: PageRef,
    /// The pages the new root no longer names, which retire with the change.
    pub replaced: Vec<String>,
}

/// The objects a branch cut at a seq no longer names: pages on the path to the cut, and pages
/// and segments wholly after it. The segment that holds the cut stays: the new branch keeps its
/// first part.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Abandoned {
    pub pages: Vec<String>,
    pub segments: Vec<String>,
}

impl<B: SessionBackend> Pages<'_, B> {
    pub async fn read(&self, page: &PageRef) -> Result<Page, DirectoryError> {
        let sealed = self
            .backend
            .get_object(self.addr, &page.object)
            .await?
            .ok_or_else(|| DirectoryError::Missing(page.object.clone()))?;
        let plain = self
            .key
            .open_unit(&aad(&self.session, &page.object), &sealed)
            .ok_or_else(|| DirectoryError::Corrupt(page.object.clone()))?;
        let decoded =
            Page::decode(&plain).map_err(|_| DirectoryError::Corrupt(page.object.clone()))?;
        if decoded.level != page.level {
            return Err(DirectoryError::Corrupt(page.object.clone()));
        }
        Ok(decoded)
    }

    async fn write(
        &self,
        name: String,
        page: &Page,
        at: Version,
    ) -> Result<PageRef, DirectoryError> {
        let sealed = self
            .key
            .seal_unit(&aad(&self.session, &name), &page.encode());
        self.backend
            .put_object(self.addr, &name, &sealed, at)
            .await?;
        Ok(page_ref(name, page))
    }

    /// Appends `entry` to the directory rooted at `root`. New pages are named
    /// `<prefix>-<n>`, so the caller's prefix must be the adding change's own.
    pub async fn append(
        &self,
        root: Option<&PageRef>,
        entry: DirEntry,
        prefix: &str,
        at: Version,
    ) -> Result<Appended, DirectoryError> {
        let mut names = 0u32;
        let mut next_name = || {
            names += 1;
            format!("{prefix}-{names}")
        };
        let Some(root) = root else {
            let leaf = Page {
                level: 0,
                entries: vec![entry],
                children: Vec::new(),
            };
            let root = self.write(next_name(), &leaf, at).await?;
            return Ok(Appended {
                root,
                replaced: Vec::new(),
            });
        };
        if entry.first_seq <= root.last_seq {
            return Err(DirectoryError::OutOfOrder);
        }
        // The rightmost path, root first.
        let mut path = vec![(root.clone(), self.read(root).await?)];
        while path.last().expect("non-empty").1.level > 0 {
            let child = path
                .last()
                .expect("non-empty")
                .1
                .children
                .last()
                .cloned()
                .ok_or_else(|| DirectoryError::Corrupt(root.object.clone()))?;
            let page = self.read(&child).await?;
            path.push((child, page));
        }
        let full = |page: &Page| {
            if page.level == 0 {
                page.entries.len() >= self.fanout
            } else {
                page.children.len() >= self.fanout
            }
        };
        // The deepest page on the path with room takes the new item. Every page below it is full,
        // so the new entry starts a fresh chain of one-item pages up to just below it.
        let room = path.iter().rposition(|(_, page)| !full(page));
        let below = room.map_or(0, |at| at + 1);
        let mut carried: Option<PageRef> = None;
        for (_, page) in path[below..].iter().rev() {
            let fresh = match carried.take() {
                None => Page {
                    level: 0,
                    entries: vec![entry.clone()],
                    children: Vec::new(),
                },
                Some(child) => Page {
                    level: page.level,
                    entries: Vec::new(),
                    children: vec![child],
                },
            };
            carried = Some(self.write(next_name(), &fresh, at).await?);
        }
        let Some(room) = room else {
            // Every page on the path was full: the old root and the fresh chain share a new
            // root, and every old page stays named, so none is replaced.
            let old_root = path[0].0.clone();
            let page = Page {
                level: old_root.level + 1,
                entries: Vec::new(),
                children: vec![old_root, carried.expect("the fresh chain was written")],
            };
            return Ok(Appended {
                root: self.write(next_name(), &page, at).await?,
                replaced: Vec::new(),
            });
        };
        // The page with room takes the item, and each of its ancestors gets its new rightmost
        // child. Those pages are the ones the new root no longer names.
        for (index, (_, page)) in path[..=room].iter_mut().enumerate().rev() {
            if index == room {
                match carried.take() {
                    None => page.entries.push(entry.clone()),
                    Some(child) => page.children.push(child),
                }
            } else {
                *page
                    .children
                    .last_mut()
                    .expect("an inner page has children") =
                    carried.take().expect("the page below was written");
            }
            carried = Some(self.write(next_name(), page, at).await?);
        }
        Ok(Appended {
            root: carried.expect("the root was written"),
            replaced: path[..=room]
                .iter()
                .map(|(page, _)| page.object.clone())
                .collect(),
        })
    }

    /// The directory rooted at `root` cut just before `cut`: segments wholly before it kept,
    /// the one holding it ended at `cut - 1`, later ones dropped. `last_ordinal` is the last
    /// message the kept part places, which the caller knows from its conversation fold. New pages
    /// are named `<prefix>-t<n>`. `None` when nothing before `cut` is sealed.
    pub async fn truncate(
        &self,
        root: Option<&PageRef>,
        cut: u64,
        last_ordinal: Option<u64>,
        prefix: &str,
        at: Version,
    ) -> Result<Option<PageRef>, DirectoryError> {
        let Some(root) = root else {
            return Ok(None);
        };
        if cut <= root.first_seq {
            return Ok(None);
        }
        if root.last_seq < cut {
            return Ok(Some(root.clone()));
        }
        let mut names = 0u32;
        let mut path = Vec::new();
        let mut page = self.read(root).await?;
        // Walk down the path to the cut, keeping each page's items before it.
        let leaf = loop {
            if page.level == 0 {
                break page;
            }
            let mut kept: Vec<PageRef> = page
                .children
                .iter()
                .filter(|child| child.first_seq < cut)
                .cloned()
                .collect();
            let straddling = kept.last().filter(|child| child.last_seq >= cut).cloned();
            let level = page.level;
            page = match straddling {
                Some(child) => {
                    kept.pop();
                    path.push((level, kept));
                    self.read(&child).await?
                }
                None => {
                    path.push((level, kept));
                    Page {
                        level: 0,
                        entries: Vec::new(),
                        children: Vec::new(),
                    }
                }
            };
        };
        let mut entries: Vec<DirEntry> = leaf
            .entries
            .into_iter()
            .filter(|entry| entry.first_seq < cut)
            .collect();
        if let Some(entry) = entries.last_mut().filter(|entry| entry.last_seq >= cut) {
            entry.last_seq = cut - 1;
            match last_ordinal
                .filter(|last| entry.first_ordinal.is_some_and(|first| first <= *last))
            {
                Some(last) => entry.last_ordinal = Some(last),
                None => {
                    entry.first_ordinal = None;
                    entry.last_ordinal = None;
                }
            }
        }
        // Rebuild upwards: each level's kept children plus the rewritten page below, if it
        // still holds anything.
        let mut carried = if entries.is_empty() {
            None
        } else {
            names += 1;
            let page = Page {
                level: 0,
                entries,
                children: Vec::new(),
            };
            Some(self.write(format!("{prefix}-t{names}"), &page, at).await?)
        };
        for (level, mut children) in path.into_iter().rev() {
            children.extend(carried.take());
            if children.is_empty() {
                continue;
            }
            names += 1;
            let page = Page {
                level,
                entries: Vec::new(),
                children,
            };
            carried = Some(self.write(format!("{prefix}-t{names}"), &page, at).await?);
        }
        Ok(carried)
    }

    /// What the directory rooted at `old` names that a branch cut before `cut` doesn't, so a
    /// revert's deletion can find it from the old root and the cut alone. Pages and segments
    /// wholly before the cut are shared with the new branch, so the walk never descends into
    /// them.
    pub async fn abandoned(
        &self,
        old: Option<&PageRef>,
        cut: u64,
    ) -> Result<Abandoned, DirectoryError> {
        let mut out = Abandoned::default();
        let mut stack: Vec<PageRef> = old.into_iter().cloned().collect();
        while let Some(page_ref) = stack.pop() {
            if page_ref.last_seq < cut {
                continue;
            }
            let page = self.read(&page_ref).await?;
            out.pages.push(page_ref.object.clone());
            stack.extend(page.children);
            for entry in page.entries {
                if entry.first_seq >= cut {
                    out.segments.push(entry.object);
                }
            }
        }
        Ok(out)
    }

    /// The entry whose segment places the message with `ordinal`, if any.
    pub async fn find_ordinal(
        &self,
        root: Option<&PageRef>,
        ordinal: u64,
    ) -> Result<Option<DirEntry>, DirectoryError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let mut page = self.read(root).await?;
        loop {
            if page.level == 0 {
                return Ok(page.entries.into_iter().find(|entry| {
                    holds_ordinal(entry.first_ordinal, entry.last_ordinal, ordinal)
                }));
            }
            let Some(child) = page
                .children
                .iter()
                .find(|child| holds_ordinal(child.first_ordinal, child.last_ordinal, ordinal))
                .cloned()
            else {
                return Ok(None);
            };
            page = self.read(&child).await?;
        }
    }

    /// The last entry whose segment starts before `seq`: the one holding the branch's latest
    /// records before it.
    pub async fn find_before(
        &self,
        root: Option<&PageRef>,
        seq: u64,
    ) -> Result<Option<DirEntry>, DirectoryError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let mut page = self.read(root).await?;
        loop {
            if page.level == 0 {
                return Ok(page
                    .entries
                    .into_iter()
                    .rev()
                    .find(|entry| entry.first_seq < seq));
            }
            let Some(child) = page
                .children
                .iter()
                .rev()
                .find(|child| child.first_seq < seq)
                .cloned()
            else {
                return Ok(None);
            };
            page = self.read(&child).await?;
        }
    }

    /// The entry whose segment holds `seq`, if any.
    pub async fn find(
        &self,
        root: Option<&PageRef>,
        seq: u64,
    ) -> Result<Option<DirEntry>, DirectoryError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let mut page = self.read(root).await?;
        loop {
            if page.level == 0 {
                return Ok(page
                    .entries
                    .into_iter()
                    .find(|entry| entry.first_seq <= seq && seq <= entry.last_seq));
            }
            let Some(child) = page
                .children
                .iter()
                .find(|child| child.first_seq <= seq && seq <= child.last_seq)
                .cloned()
            else {
                return Ok(None);
            };
            page = self.read(&child).await?;
        }
    }
}

#[cfg(test)]
mod tests;
