//! Predicate-bounded ledger reads, from the open tail through sealed blocks newest first.

use crate::backend::SessionBackend;
use crate::directory::Pages;
use crate::record::{Record, Stream};
use crate::segment::Segment;

use super::{Session, SessionError};

impl<B: SessionBackend> Session<B> {
    /// Ledger records newest first, including the first record for which `stop` returns true.
    /// A retained open-log suffix is visited without cloning the rest of its run. If it is
    /// insufficient, the open log is read at most once, then sealed ledger segments and their
    /// bounded blocks are read newest first. No older segment is fetched after the predicate
    /// succeeds. A transcript record never satisfies the predicate or enters the result.
    pub async fn ledger_tail_until(
        &self,
        mut stop: impl FnMut(&Record) -> bool,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut out = Vec::new();
        let (kept, whole) = self.kept.tail();
        for (seq, record) in kept
            .iter()
            .rev()
            .filter(|(_, r)| r.stream() == Stream::Ledger)
        {
            out.push((*seq, record.clone()));
            if stop(record) {
                return Ok(out);
            }
        }
        if !whole {
            self.storage_read()?;
            let first_kept = kept.first().map(|(seq, _)| *seq).unwrap_or(u64::MAX);
            for (seq, record) in self.place().open_log().await?.records.into_iter().rev() {
                if seq >= first_kept || record.stream() != Stream::Ledger {
                    continue;
                }
                let done = stop(&record);
                out.push((seq, record));
                if done {
                    return Ok(out);
                }
            }
        }
        let Some(root) = self.head.ledger_dir.as_ref() else {
            return Ok(out);
        };
        self.storage_read()?;
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut seen: std::collections::BTreeSet<_> = out.iter().map(|(seq, _)| *seq).collect();
        let mut cursor = root.last_seq;
        while let Some(entry) = pages
            .last_at_or_before(Some(root), cursor)
            .await
            .map_err(SessionError::Directory)?
        {
            let bytes = self
                .backend
                .get_object(&self.addr, &entry.object)
                .await?
                .ok_or_else(|| {
                    SessionError::Fold(format!(
                        "the directory names a missing segment {}",
                        entry.object
                    ))
                })?;
            let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                .map_err(SessionError::Segment)?;
            for position in (0..segment.index.blocks.len()).rev() {
                if segment.index.blocks[position].first_seq > cursor {
                    continue;
                }
                for (seq, record) in segment
                    .block(position)
                    .map_err(SessionError::Segment)?
                    .into_iter()
                    .rev()
                {
                    if seq > cursor || !entry.keeps(seq) || record.stream() != Stream::Ledger {
                        continue;
                    }
                    // A seal can retain records in the open log; never return one twice.
                    if !seen.insert(seq) {
                        continue;
                    }
                    let done = stop(&record);
                    out.push((seq, record));
                    if done {
                        return Ok(out);
                    }
                }
            }
            let Some(before) = entry.first_seq.checked_sub(1) else {
                break;
            };
            cursor = before;
        }
        Ok(out)
    }
}
