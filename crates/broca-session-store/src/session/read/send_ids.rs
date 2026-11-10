//! Read-only recovery of send ids from the records that accepted and placed each input.

use std::collections::{BTreeMap, BTreeSet};

use crate::backend::SessionBackend;
use crate::fold::conversation::Conversation;
use crate::record::{Placement, Record};

use super::{Session, SessionError, StoredEntry};

pub(super) struct SendIdIndex {
    through: u64,
    conversation: Conversation,
    run: Option<(String, Option<String>)>,
    queued: BTreeMap<String, String>,
    receipts: BTreeMap<u64, Option<String>>,
    entries: BTreeMap<u64, String>,
    steers: BTreeSet<u64>,
}

impl SendIdIndex {
    fn new(appends: bool) -> Self {
        Self {
            through: 0,
            conversation: Conversation::new(appends),
            run: None,
            queued: BTreeMap::new(),
            receipts: BTreeMap::new(),
            entries: BTreeMap::new(),
            steers: BTreeSet::new(),
        }
    }

    fn observe(&mut self, seq: u64, record: &Record) -> Result<(), SessionError> {
        match record {
            Record::RunStart(start) => {
                let fresh = start
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.get("kind"))
                    .and_then(serde_json::Value::as_str)
                    == Some("fresh");
                let id = fresh
                    .then(|| {
                        start
                            .send_id
                            .clone()
                            .or_else(|| {
                                start
                                    .submission_id
                                    .as_ref()
                                    .and_then(|id| self.queued.get(id).cloned())
                            })
                            .or_else(|| start.submission_id.clone())
                    })
                    .flatten();
                self.run = Some((start.run_id.clone(), id));
            }
            Record::QueuedSend(send) => {
                self.queued.insert(
                    send.submission_id.clone(),
                    send.send_id
                        .clone()
                        .unwrap_or_else(|| send.submission_id.clone()),
                );
            }
            Record::Steer(steer) => {
                let id = steer
                    .receipt
                    .as_ref()
                    .and_then(|receipt| receipt.get("send_id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                self.receipts.insert(seq, id);
            }
            Record::Delivery(delivery) => {
                self.receipts
                    .insert(seq, self.queued.get(&delivery.submission_id).cloned());
            }
            _ => {}
        }
        let id = match record {
            Record::Input(input) => match input.placement {
                Placement::RunInput => self
                    .run
                    .as_ref()
                    .filter(|(run, _)| run == &input.run_id)
                    .and_then(|(_, id)| id.clone()),
                Placement::Steer => input
                    .receipt_seq
                    .and_then(|seq| self.receipts.get(&seq))
                    .cloned()
                    .flatten(),
                Placement::Delivered | Placement::DeliveredTurnSystem => input
                    .submission_id
                    .as_ref()
                    .and_then(|id| self.queued.get(id).cloned()),
                Placement::Imported | Placement::BudgetNudge | Placement::DamageNotice => None,
            },
            _ => None,
        };
        let before = self.conversation.placed;
        if record.stream() == crate::record::Stream::Transcript {
            self.conversation
                .apply(record)
                .map_err(|error| SessionError::Fold(format!("send ids: conversation {error:?}")))?;
        }
        if let Some(id) = id.filter(|_| self.conversation.placed == before + 1) {
            self.entries.insert(self.conversation.placed, id);
            if matches!(record, Record::Input(input) if input.placement == Placement::Steer) {
                self.steers.insert(self.conversation.placed);
            }
        }
        if let Record::EntryFacts(facts) = record {
            // Older conversions wrote steer receipt positions before accounting for the opening
            // record. Even a reference to a valid adjacent steer can therefore name the wrong send.
            // Native ids encode placement as g<generation>.<seq>. Other ids may come from a
            // conversion, so a derived steer id is not trustworthy without stored send facts.
            if self.steers.contains(&facts.ordinal) && super::minted_seq(&facts.mid).is_none() {
                self.entries.remove(&facts.ordinal);
            }
        }
        self.conversation.clear_window();
        self.through = seq;
        Ok(())
    }
}

impl<B: SessionBackend> Session<B> {
    /// Caller-visible send ids for entries whose old facts do not carry one. The first lookup
    /// builds a session-local index by folding the actual input placements and their receipts;
    /// later pages fold only new records. A revert drops the index with the other read caches.
    /// Imported entries whose source id was discarded remain unknown, never the import's id.
    pub async fn older_entry_send_ids(
        &self,
        entries: &[StoredEntry],
    ) -> Result<BTreeMap<u64, String>, SessionError> {
        let needed: Vec<_> = entries
            .iter()
            .filter(|entry| {
                entry
                    .state
                    .facts
                    .as_ref()
                    .is_some_and(|facts| facts.sender.is_some() && facts.send_id.is_none())
            })
            .map(|entry| entry.ordinal)
            .collect();
        if needed.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut index = self.sealed_caches().send_ids.take().unwrap_or_else(|| {
            Box::new(SendIdIndex::new(self.conversation.appends_in_conversation))
        });
        let last = self.next_seq() - 1;
        if index.through < last {
            for (seq, record) in self.records_between(index.through + 1, last).await? {
                index.observe(seq, &record)?;
            }
        }
        let found = needed
            .into_iter()
            .filter_map(|ordinal| index.entries.get(&ordinal).cloned().map(|id| (ordinal, id)))
            .collect();
        self.sealed_caches().send_ids = Some(index);
        Ok(found)
    }
}
