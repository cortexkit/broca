//! Today's side of the converter's history check: `session.history` for a session closed on
//! today's storage, answered from its WAL alone by today's own steps (its pages by `Todays`, its
//! log, part numbers and frozen plan from the records). It is a serve handler, so
//! `crate::history::read` runs over it unchanged, as it runs over the new storage's side.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_wal::{FramedRecord, WalRecord};
use tokio::sync::mpsc;

use super::todays::Todays;
use crate::serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, ServeError, ServeHandler,
    SessionReadRequest, StatusResponse, SubscribeFrom,
};
use crate::LineageKey;

/// `session.history` on today's storage for a closed session whose WAL is `records`.
pub struct TodaysHistory<'a> {
    todays: Mutex<Todays<'a>>,
    records: Vec<WalRecord>,
    head: broca_wire::HeadResponse,
    manifest: Option<Arc<broca_wal::PlanManifest>>,
}

impl<'a> TodaysHistory<'a> {
    /// Today's history of the session whose WAL is `records`, with lineage `lineage`. Its head
    /// names `incarnation` and `history_version`, which each storage keeps for itself, so the
    /// check passes the store's: cursors then compare as the positions they hold.
    pub fn new(
        records: &'a [FramedRecord],
        lineage: Option<&'a str>,
        incarnation: String,
        history_version: u64,
    ) -> Result<Self, ServeError> {
        let plain: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
        // Today's head reads its ordinals from the newest one-message page.
        let (page, _) = broca_core::read_session_page_owned_with_dispatches(
            broca_core::StorePageSnapshot::empty(),
            records,
            None,
            Some(1),
        )
        .map_err(|error| ServeError::Open(format!("replay failed: {error}"), None))?;
        let head = broca_wire::HeadResponse {
            incarnation,
            history_version,
            last_ordinal: (page.prompt_len > 0).then_some(page.prompt_len),
            first_ordinal: (page.prompt_len > 0).then_some(1),
            mode3: broca_core::is_mode3_log(&plain),
            lineage_id: lineage.map(str::to_owned),
            ..Default::default()
        };
        // The plan the session froze at admission: its first run's, as today's ownership keeps it.
        let manifest = plain.iter().find_map(|record| match record {
            WalRecord::RunStarted { manifest, .. } => {
                Some(manifest.as_deref().cloned().map(Arc::new))
            }
            _ => None,
        });
        Ok(Self {
            todays: Mutex::new(Todays::new(records, lineage)),
            records: plain,
            head,
            manifest: manifest.flatten(),
        })
    }

    /// This history with the replies at `ordinals` (zero-based) served cleaned of tag notation,
    /// as [`Todays::with_clean_replies`] serves its pages.
    pub fn with_clean_replies(self, ordinals: std::collections::BTreeSet<u64>) -> Self {
        let todays = self.todays.into_inner().expect("today's reads");
        Self {
            todays: Mutex::new(todays.with_clean_replies(ordinals)),
            ..self
        }
    }
}

#[async_trait]
impl ServeHandler for TodaysHistory<'_> {
    async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
        unreachable!("session.history sends nothing")
    }
    async fn retract(
        &self,
        _: &LineageKey,
        _: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unreachable!("session.history retracts nothing")
    }
    async fn subscribe(
        &self,
        _: &LineageKey,
        _: SubscribeFrom,
        _: mpsc::Sender<crate::Delivery>,
    ) -> Result<(), ServeError> {
        unreachable!("session.history subscribes to nothing")
    }
    async fn cancel(
        &self,
        _: &LineageKey,
        _: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unreachable!("session.history cancels nothing")
    }
    async fn status(&self, _: &LineageKey) -> Result<StatusResponse, ServeError> {
        unreachable!("session.history reads no status")
    }
    async fn run_status(
        &self,
        _: &LineageKey,
        _: broca_protocol::RunId,
    ) -> Result<broca_wire::RunStatusResponse, ServeError> {
        unreachable!("session.history reads no run status")
    }

    async fn read(
        &self,
        req: SessionReadRequest,
    ) -> Result<broca_wire::SessionReadResponse, ServeError> {
        self.todays.lock().expect("today's reads").read(&req)
    }

    async fn head(&self, _: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        Ok(self.head.clone())
    }

    /// This history stands for the store's session, whose served version it was given: its
    /// output-reference ids are scoped to the version that session stores, as the store's are.
    async fn output_ref_history_version(
        &self,
        _: &LineageKey,
        served: u64,
    ) -> Result<u64, ServeError> {
        crate::store_exposure::stored_history_version(served).ok_or(ServeError::HistoryChanged)
    }

    async fn session_log(
        &self,
        _: &LineageKey,
        _: &str,
    ) -> Result<Option<Vec<WalRecord>>, ServeError> {
        Ok(Some(self.records.clone()))
    }

    async fn frozen_manifest(
        &self,
        _: &LineageKey,
    ) -> Result<Option<Arc<broca_wal::PlanManifest>>, ServeError> {
        Ok(self.manifest.clone())
    }
}
