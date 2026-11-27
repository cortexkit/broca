//! Bounded ordinal pages served by an open actor.

use super::*;

pub(super) async fn after<B: SessionBackend>(
    session: &Session<B>,
    full_request: &ReadRequest,
    has_byte_cap: bool,
    finish: impl Fn(ReadPage) -> ReadPage,
) -> Result<ReadPage, String> {
    let Select::After {
        after_ordinal,
        limit,
    } = full_request.select
    else {
        unreachable!("only ordinal pages use the chunked reader");
    };
    let _timing = broca_session_store::metrics::Phase::EntriesPage
        .start()
        .with_session(|| Some(session.address().0.clone()));
    let len = transcript_len(session);
    let total = (limit.is_some() || !has_byte_cap).then(|| count_with_lookahead(limit));
    let mut from = after_ordinal.unwrap_or(0).saturating_add(1);
    let mut entries = Vec::new();
    loop {
        let count = session
            .compaction_page_chunk_size(entries.len(), total)
            .map_err(|error| format!("{error:?}"))?;
        let stored = crate::store_read::served_compaction_entries(session, from, count)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let exhausted =
            stored.len() < count || stored.last().is_some_and(|last| last.ordinal >= len);
        for entry in &stored {
            entries.push(project(entry)?);
        }

        let finished = match total {
            Some(total) if exhausted || entries.len() == total => Some(finish(
                broca_core::compaction_v2::views::page(&entries, full_request, Head::new()),
            )),
            Some(_) => None,
            None => {
                // The last entry of the batch is also look-ahead: if it doesn't fit, the
                // existing byte-cap logic produces the continuation without another read.
                // If everything fits, keep the prefix and read only the next batch.
                let page = finish(ReadPage::new(entries.clone(), Head::new()));
                (exhausted || page.entries.len() < entries.len()).then_some(page)
            }
        };
        if let Some(mut page) = finished {
            page.head = entries_head(session, len, entries.last()).await?;
            return Ok(page);
        }
        from = entries.last().expect("a full batch has entries").ordinal + 1;
    }
}
