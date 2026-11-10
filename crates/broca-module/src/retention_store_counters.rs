//! Counter carryover follows the legacy WAL's expiry tombstones under `wal-ephemeral/`,
//! not the current projection's numbering.

use broca_session_store::record::{Placement, Record};

pub(crate) fn next_tag(records: &[(u64, Record)]) -> Result<Option<u64>, String> {
    let mut first = None;
    for (_, record) in records {
        if let Record::RunStart(start) = record {
            // Today's LineageTags switches to Computed on such a start and then carries no
            // next ledger tag, even if earlier runs in the same session stamped ledger tags.
            if start
                .run_choices
                .as_ref()
                .and_then(|choices| choices.pointer("/config/computed_tags"))
                .is_some_and(|tags| !tags.is_null())
            {
                return Ok(None);
            }
            first.get_or_insert(start.run_id.as_str());
        }
    }
    let tagged = records.iter().any(|(_, record)| {
        matches!(record,
            Record::Input(input) if Some(input.run_id.as_str()) == first
                && input.placement == Placement::RunInput && input.tags_v1.is_some()
        )
    });
    if !tagged {
        return Ok(None);
    }
    let mut high = 0;
    for (_, record) in records {
        let (tags, many) = match record {
            Record::Input(input) => (input.tags_v1.as_ref(), false),
            Record::Assistant(assistant) => (assistant.tags_v1.as_ref(), false),
            Record::ToolReply(reply) => (reply.tags_v1.as_ref(), false),
            Record::Steer(steer) => (steer.tags_v1.as_ref(), true),
            Record::Delivery(delivery) => (delivery.tags_v1.as_ref(), true),
            Record::BudgetFire(fire) => (fire.tags_v1.as_ref(), false),
            _ => (None, false),
        };
        if let Some(tags) = tags {
            let messages = if many {
                serde_json::from_value::<Vec<broca_protocol::tags::MessageTags>>(tags.clone())
            } else {
                serde_json::from_value::<broca_protocol::tags::MessageTags>(tags.clone())
                    .map(|tags| vec![tags])
            }
            .map_err(|error| format!("the expired session's ledger tags: {error}"))?;
            high = high.max(
                messages
                    .iter()
                    .filter_map(|tags| tags.max_tag())
                    .max()
                    .unwrap_or(0),
            );
        }
    }
    Ok(Some(high.saturating_add(1)))
}
