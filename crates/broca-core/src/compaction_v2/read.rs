//! Indexed reads use a bounded native page and only the unprojected WAL suffix.

use super::protocol::{
    read::{Head, ReadPage, ReadRequest, Select},
    view::AppliedView,
};
use broca_protocol::BindIdentity;
use std::sync::Arc;

pub async fn entries<W: crate::WalStore>(
    wal: &W,
    store: Option<Arc<dyn crate::SessionStore>>,
    session: &BindIdentity,
    writer: &crate::WalHandle,
    request: &ReadRequest,
    applied: Option<AppliedView>,
) -> Result<ReadPage, String> {
    let safe = writer.sync().await.map_err(|error| error.to_string())?;
    let lineage = writer
        .lineage_id()
        .ok_or("compaction session has no WAL lineage")?
        .to_string();
    if let Some(store) = store {
        crate::run::project_committed(wal, store.clone(), session, safe, false).await;
        if let Some(page) = store.read_compaction_page(session, request)? {
            let Some(seed) = store.projection_seed(session)? else {
                if page.prefix_len != 0 {
                    return Err("compaction projection has no checkpoint".into());
                }
                return wal_page(wal, session, request, applied, safe, &lineage);
            };
            if seed.prefix_len == 0
                && seed
                    .admission_checkpoint
                    .head
                    .as_ref()
                    .and_then(|head| head.compaction_metadata.as_ref())
                    .is_none()
            {
                return wal_page(wal, session, request, applied, safe, &lineage);
            }
            if seed.watermark_seq != page.watermark_seq {
                return Err("compaction projection changed while reading the page".into());
            }
            if page.lineage_id.as_deref() != Some(lineage.as_str()) {
                return Err("compaction projection lineage does not match the session".into());
            }
            let range = wal
                .read_records_range(
                    session,
                    page.watermark_seq,
                    Some(safe),
                    page.anchor.as_ref(),
                )
                .map_err(|error| error.to_string())?;
            let tail = super::transcript::tail(
                &seed,
                super::host::range_records(range.records())?,
                &lineage,
            )?;
            let mut head = Head::new();
            head.applied_view = applied;
            head.newest = super::views::newest(&tail.entries)
                .or_else(|| page.head.and_then(|head| head.compaction_newest));
            let mut selected = page.entries;
            if !page.has_more {
                selected.extend(tail.entries);
            }
            let mut result = super::views::page(&selected, request, head);
            if page.has_more && matches!(request.select, Select::After { .. }) {
                result.next_after = result.entries.last().map(|entry| entry.ordinal);
            }
            return Ok(result);
        }
    }
    // Retained lineages and dormant stores have no materialized compaction
    // protocol Entry pages, so they replay the native transcript from the WAL.
    wal_page(wal, session, request, applied, safe, &lineage)
}

// The first native turn may not have reached a projectable boundary yet. Its
// bootstrap read shares the WAL fallback used by stores without Entry pages.
fn wal_page(
    wal: &dyn crate::WalStore,
    session: &BindIdentity,
    request: &ReadRequest,
    applied: Option<AppliedView>,
    safe: u64,
    lineage: &str,
) -> Result<ReadPage, String> {
    let range = wal
        .read_records_range(session, 0, Some(safe), None)
        .map_err(|error| error.to_string())?;
    let transcript =
        super::transcript::project(super::host::range_records(range.records())?, lineage)?;
    let mut head = Head::new();
    head.newest = super::views::newest(&transcript.entries);
    head.applied_view = applied;
    Ok(super::views::page(&transcript.entries, request, head))
}
