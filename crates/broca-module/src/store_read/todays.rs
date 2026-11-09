//! Today's `session.read` reply for a session closed on today's storage, built from its WAL
//! alone by today's own steps (`LineageRegistry::read`): what the converter's page check
//! compares the new storage's reply with, page by page.
//!
//! The module serves a read through more paths than a WAL: the materialized store's rows, the
//! display-facts cache and a live actor. For a closed session with neither rows nor cache they
//! reduce to the steps here, each the same function today's read calls. What a read takes from
//! the registry beyond the WAL (its history version, its approval view, a subscribe cursor's
//! frame identity) is left as the page builder leaves it.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, HashMap};

use broca_wal::{FramedRecord, WalRecord};

use crate::serve::{ServeError, SessionReadRequest};

type Facts = BTreeMap<u64, broca_core::display_facts::DisplayFacts>;
type Dispatches = BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>;
/// The pages built so far, by the 0-based ordinal they start from and their limit.
type Pages = HashMap<(Option<u64>, Option<u64>), broca_core::SessionReadPage>;

/// Today's reads of one closed session, whose whole WAL is `records`. What every read of the
/// session derives alike (each page, and the display facts folded from the log's start) is
/// derived once and kept, so walking every page in several read modes replays the log once
/// per page, not once per page and mode.
pub struct Todays<'a> {
    records: &'a [FramedRecord],
    lineage: Option<&'a str>,
    mode3: bool,
    rides: bool,
    pages: RefCell<Pages>,
    /// The dispatch facts of every tool turn the log commits, which the page builder answers
    /// beside each page whatever its bounds.
    dispatches: OnceCell<Dispatches>,
    /// The dispatch facts of every committed tool turn of the whole log, which today's read takes
    /// a turn's from when the page's own don't hold it.
    whole: OnceCell<Dispatches>,
    facts: OnceCell<Facts>,
}

impl<'a> Todays<'a> {
    /// Today's reads of the session whose WAL is `records`, with lineage `lineage`.
    pub fn new(records: &'a [FramedRecord], lineage: Option<&'a str>) -> Self {
        Self {
            records,
            lineage,
            mode3: broca_core::is_mode3_log(records.iter().map(|frame| &frame.record)),
            rides: records.iter().any(|frame| {
                matches!(
                    &frame.record,
                    WalRecord::SendDelivered { ride: Some(_), .. }
                )
            }),
            pages: RefCell::default(),
            dispatches: OnceCell::new(),
            whole: OnceCell::new(),
            facts: OnceCell::new(),
        }
    }

    /// Today's reply to `req`.
    pub fn read(
        &self,
        req: &SessionReadRequest,
    ) -> Result<broca_wire::SessionReadResponse, ServeError> {
        let from_ordinal = req.from_ordinal.map(|at| at.saturating_sub(1));
        let (page, dispatches) = self.page(from_ordinal, req.limit)?;
        let tools = if req.include_tools {
            crate::registry::latest_run_started_tools(self.records)
        } else {
            None
        };
        let times = if req.include_display_facts {
            crate::serve::DisplayTimes::of(&page)
        } else {
            crate::serve::DisplayTimes::default()
        };
        let mut response = crate::serve::session_read_response(page, tools);
        response.lineage_id = self.lineage.map(str::to_owned);
        // Today's read folds display facts for an opted-in read, and for the tool results of a
        // log whose sends rode on them, whose native outputs it serves instead of the joined
        // ones.
        let none = Facts::new();
        let facts = match self.lineage {
            Some(_) if req.include_display_facts || self.rides => self.facts()?,
            _ => &none,
        };
        if self.rides {
            for entry in &mut response.messages {
                let Some(outputs) = facts
                    .get(&(entry.ordinal - 1))
                    .map(|facts| &facts.native_tool_outputs)
                else {
                    continue;
                };
                for (part, block) in entry.message.content.iter_mut().enumerate() {
                    if let broca_protocol::ContentBlock::ToolResult(result) = block {
                        if let Some(output) = outputs.get(&(part as u32)) {
                            result.output = output.clone();
                        }
                    }
                }
            }
        }
        if req.include_display_facts && response.lineage_id.is_some() {
            crate::serve::apply_display_facts(&mut response, &times, facts, None);
        }
        if req.include_rich_ui {
            let stamps = crate::serve::rich_ui_runs(&response)
                .into_iter()
                .map(|run_id| {
                    let stamp = self.records.iter().find_map(|frame| match &frame.record {
                        WalRecord::RunStarted {
                            run_id: started,
                            manifest,
                            ..
                        } if started.0 == run_id => manifest
                            .as_ref()
                            .and_then(|manifest| manifest.rich_ui.clone()),
                        _ => None,
                    });
                    (run_id, stamp)
                })
                .collect();
            crate::serve::apply_rich_ui(&mut response, &stamps);
        }
        self.attribute_tool_calls(&mut response, dispatches)?;
        crate::serve::apply_media_mode(&mut response, req.media, &req.session.session)?;
        for entry in &mut response.messages {
            entry.mid = broca_core::message_mid(self.mode3, self.lineage, entry.ordinal - 1)
                .map_err(|error| {
                    ServeError::Open(error, Some(broca_protocol::ErrorClass::Permanent))
                })?;
        }
        let cap = req
            .max_bytes
            .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
            .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
        crate::serve::cap_page_bytes(
            &mut response,
            cap,
            from_ordinal.is_none(),
            &req.session.session,
        )?;
        Ok(response)
    }

    /// Fills each message's `tool_calls`, and a carrier result's call key, as today's read does
    /// (`LineageRegistry::attribute_tool_calls`): from the dispatch facts of the turns the page
    /// asks about, the page's own (`tail`) else the whole log's. Without a lineage no call has a
    /// key, so every call is served with no dispatch fact.
    fn attribute_tool_calls(
        &self,
        response: &mut broca_wire::SessionReadResponse,
        tail: &Dispatches,
    ) -> Result<(), ServeError> {
        let asked: Vec<u64> = response
            .messages
            .iter()
            .filter_map(|message| {
                let delivered = message
                    .run
                    .as_ref()
                    .and_then(|run| run.delivery.as_ref())
                    .is_some_and(|delivery| {
                        delivery.mode == broca_protocol::DeliveryMode::ToolResult
                    });
                let calls = message
                    .message
                    .content
                    .iter()
                    .any(|block| matches!(block, broca_protocol::ContentBlock::ToolCall(_)));
                if delivered {
                    message.ordinal.checked_sub(2)
                } else if calls {
                    message.ordinal.checked_sub(1)
                } else {
                    None
                }
            })
            .collect();
        if asked.is_empty() {
            return Ok(());
        }
        let Some(lineage) = self.lineage else {
            crate::serve::apply_tool_call_dispatches(response, &Dispatches::new(), "");
            return Ok(());
        };
        let whole = match self.whole.get() {
            Some(whole) => whole,
            None => {
                let whole = broca_core::tool_call_dispatches(self.records)
                    .map_err(|error| ServeError::Open(format!("replay failed: {error}"), None))?;
                self.whole.get_or_init(|| whole)
            }
        };
        let picked: Dispatches = asked
            .iter()
            .filter_map(|ordinal| {
                tail.get(ordinal)
                    .or_else(|| whole.get(ordinal))
                    .map(|facts| (*ordinal, facts.clone()))
            })
            .collect();
        crate::serve::apply_tool_call_dispatches(response, &picked, lineage);
        Ok(())
    }

    /// The page today's builder assembles from the WAL, with the dispatch facts of the tool
    /// turns it commits, as the read's page route answers it for a session with no store rows.
    fn page(
        &self,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
    ) -> Result<(broca_core::SessionReadPage, &Dispatches), ServeError> {
        if let Some(page) = self.pages.borrow().get(&(from_ordinal, limit)) {
            if let Some(dispatches) = self.dispatches.get() {
                return Ok((page.clone(), dispatches));
            }
        }
        let (page, dispatches) = broca_core::read_session_page_owned_with_dispatches(
            broca_core::StorePageSnapshot::empty(),
            self.records,
            from_ordinal,
            limit,
        )
        .map_err(|error| ServeError::Open(format!("replay failed: {error}"), None))?;
        self.pages
            .borrow_mut()
            .insert((from_ordinal, limit), page.clone());
        Ok((page, self.dispatches.get_or_init(|| dispatches)))
    }

    /// Today's display facts of every message of the log, by zero-based ordinal, as the read's
    /// sidecar folds them from the log's start, with the notices its send-key index rules out
    /// removed (`LineageRegistry::display_facts_for_page`).
    fn facts(&self) -> Result<&Facts, ServeError> {
        if let Some(facts) = self.facts.get() {
            return Ok(facts);
        }
        let lineage = self.lineage.unwrap_or_default();
        let mut seed = broca_core::display_facts::DisplayFactsSeed::default();
        let versions = seed
            .fold(self.records, lineage)
            .map_err(|error| ServeError::Open(error, None))?;
        let mut rows = Facts::new();
        for version in versions {
            rows.insert(version.ordinal, version.facts);
        }
        for facts in rows.values_mut() {
            facts.resolve_notices(&seed.send_keys).map_err(|reason| {
                ServeError::Open(reason, Some(broca_protocol::ErrorClass::Transient))
            })?;
        }
        Ok(self.facts.get_or_init(|| rows))
    }
}
