//! Following the daemon's catalog generation, so `catalog.list` is read only when the catalog
//! changed.
//!
//! The daemon's `catalog.watch` pushes only a generation number: the current one first, then
//! each later one, possibly skipping some. It never carries the catalog itself, and it ends with
//! the socket. A connection that follows the catalog ([`SubcConnection::follow_catalog`]) keeps
//! its last `catalog.list` reply with that reply's generation and serves the reply again while
//! the latest pushed generation is the same and the socket that read it is still the current
//! one. Anything else reads `catalog.list` again: no push yet, a newer push, a reconnect, a
//! connection that doesn't follow the catalog, or a daemon that refuses the watch.
//!
//! A registered module's readiness can change without a new generation, so a held reply's
//! `ready` members may be old. Nothing broca reads from the catalog depends on them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use subc_control::{
    CatalogEntry, CatalogWatchRequest, CatalogWatchResponse, ClientControlRequest,
    ClientControlResponse,
};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority, FLAG_SUBSCRIPTION};
use tokio::sync::mpsc;

use super::{control_flags, ConnError, ConnResult, ConnToken, PendingRegistration, SubcConnection};

/// The first wait before watching again after a watch ended or could not start.
const WATCH_RETRY_FIRST: Duration = Duration::from_millis(500);
/// The longest wait between attempts, reached when the daemon keeps refusing the watch.
const WATCH_RETRY_MAX: Duration = Duration::from_secs(60);

/// The catalog a following connection serves now: the socket its watch is open on and the
/// generation the daemon last pushed there. Two equal values name the same catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CatalogGeneration {
    conn: ConnToken,
    generation: u64,
}

impl CatalogGeneration {
    #[cfg(test)]
    pub(crate) fn for_test(generation: u64) -> Self {
        Self {
            conn: ConnToken(0),
            generation,
        }
    }
}

/// The last `catalog.list` reply and the generation the daemon last pushed.
#[derive(Default)]
pub(super) struct CatalogMemo {
    /// Set once [`SubcConnection::follow_catalog`] starts. Until then reads run side by side as
    /// they always did, with no shared read to wait for.
    following: AtomicBool,
    state: Mutex<MemoState>,
    /// One `catalog.list` read at a time, so callers that find the reply stale together share
    /// the next read instead of each sending one.
    read: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct MemoState {
    /// The socket and generation of the latest push, while that socket's watch is open.
    pushed: Option<(ConnToken, u64)>,
    /// The last `catalog.list` reply, with the socket that read it and its generation.
    /// The reply contains parsed JSON schemas, shared through the `Arc` instead of copied.
    held: Option<(ConnToken, u64, Arc<Vec<CatalogEntry>>)>,
}

impl CatalogMemo {
    /// The held reply, when it is still the catalog `current` serves.
    fn current(&self, current: ConnToken) -> Option<Arc<Vec<CatalogEntry>>> {
        let state = self.state.lock().unwrap();
        match (&state.pushed, &state.held) {
            (Some((pushed_on, pushed)), Some((read_on, read, entries)))
                if *pushed_on == current && *read_on == current && pushed == read =>
            {
                Some(Arc::clone(entries))
            }
            _ => None,
        }
    }

    fn hold(&self, read_on: ConnToken, generation: u64, entries: Arc<Vec<CatalogEntry>>) {
        self.state.lock().unwrap().held = Some((read_on, generation, entries));
    }

    fn pushed(&self, on: ConnToken, generation: u64) {
        self.state.lock().unwrap().pushed = Some((on, generation));
    }

    /// The watch on `on` ended: its pushes say nothing about the catalog any more.
    fn watch_ended(&self, on: ConnToken) {
        let mut state = self.state.lock().unwrap();
        if state.pushed.is_some_and(|(pushed_on, _)| pushed_on == on) {
            state.pushed = None;
        }
    }
}

impl SubcConnection {
    /// `catalog.list`: registered providers and their tools. On a connection that follows the
    /// catalog, the last reply while its generation is still current, shared rather than
    /// copied; otherwise read now.
    pub async fn catalog_list(&self) -> ConnResult<Arc<Vec<CatalogEntry>>> {
        let memo = &self.shared.catalog;
        if !memo.following.load(Ordering::Acquire) {
            return self
                .read_catalog()
                .await
                .map(|(_, _, entries)| Arc::new(entries));
        }
        if let Some(entries) = memo.current(self.current_conn()) {
            return Ok(entries);
        }
        let _read = memo.read.lock().await;
        if let Some(entries) = memo.current(self.current_conn()) {
            return Ok(entries);
        }
        let (read_on, generation, entries) = self.read_catalog().await?;
        let entries = Arc::new(entries);
        memo.hold(read_on, generation, Arc::clone(&entries));
        Ok(entries)
    }

    /// Follow the daemon's catalog generation for as long as this connection lives, so
    /// [`Self::catalog_list`] reads the catalog again only when it changed. A watch that ends
    /// (a reconnect, a refusal, a daemon without `catalog.watch`) is opened again after a wait
    /// that grows to a minute; until then every `catalog_list` reads, as it does on a connection
    /// that doesn't follow.
    pub fn follow_catalog(self: &Arc<Self>) {
        if self.shared.catalog.following.swap(true, Ordering::AcqRel) {
            return;
        }
        let connection = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut retry = WATCH_RETRY_FIRST;
            // The watch holds the connection only while it opens and while it records a push,
            // so the connection still closes when its last owner drops it, which ends the watch.
            while let Some(ended) = Self::watch_catalog(&connection).await {
                match ended {
                    // A watch that delivered pushes was working: the next one starts soon.
                    Ok(true) => retry = WATCH_RETRY_FIRST,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::debug!(target: "subc.catalog", %error, "catalog watch ended")
                    }
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(WATCH_RETRY_MAX);
            }
        });
    }

    /// One `catalog.watch` on the current socket, recording each pushed generation until the
    /// feed ends. True when the feed delivered at least one push; `None` once the connection is
    /// gone.
    async fn watch_catalog(connection: &Weak<Self>) -> Option<ConnResult<bool>> {
        let (on, _registration, mut rx) = match connection.upgrade()?.open_catalog_watch().await {
            Ok(opened) => opened,
            Err(error) => return Some(Err(error)),
        };
        let mut delivered = false;
        let ended = loop {
            // The registration is dropped with the socket, which ends this read with `None`.
            let Some(frame) = rx.recv().await else {
                break Ok(delivered);
            };
            let owner = connection.upgrade()?;
            match frame.header.ty {
                FrameType::Response => {
                    match serde_json::from_slice::<CatalogWatchResponse>(&frame.body) {
                        Ok(push) => {
                            owner.shared.catalog.pushed(on, push.generation);
                            delivered = true;
                        }
                        Err(error) => {
                            break Err(ConnError::Protocol(format!(
                                "decode catalog.watch push: {error}"
                            )))
                        }
                    }
                    if frame.header.flags.is_last() {
                        break Ok(delivered);
                    }
                }
                FrameType::Error => {
                    let refusal = serde_json::from_slice::<ErrorBody>(&frame.body)
                        .map(|body| format!("{}: {}", body.code, body.message))
                        .unwrap_or_else(|_| "undecodable refusal".into());
                    break Err(ConnError::Protocol(format!(
                        "catalog.watch refused: {refusal}"
                    )));
                }
                _ => break Ok(delivered),
            }
        };
        connection.upgrade()?.shared.catalog.watch_ended(on);
        Some(ended)
    }

    /// Send `catalog.watch` on the current socket: the socket, and the registration and
    /// receiver its pushes arrive on.
    async fn open_catalog_watch(
        &self,
    ) -> ConnResult<(
        ConnToken,
        PendingRegistration,
        mpsc::UnboundedReceiver<Frame>,
    )> {
        let body = serde_json::to_vec(&CatalogWatchRequest::new())
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        let corr = self.next_corr();
        let (runtime, registration, rx) = self.register_subscription((0, 0, corr))?;
        let flags = Flags(Flags::new(false, Priority::Passive, false).0 | FLAG_SUBSCRIPTION);
        let frame = Frame::build(FrameType::Request, flags, 0, 0, corr, body)
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        self.send(&runtime, frame).await?;
        Ok((runtime.conn, registration, rx))
    }

    /// Read `catalog.list` now, with the socket that answered and the reply's generation.
    async fn read_catalog(&self) -> ConnResult<(ConnToken, u64, Vec<CatalogEntry>)> {
        let body = serde_json::to_vec(&ClientControlRequest::CatalogList { module_id: None })
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        let corr = self.next_corr();
        let (runtime, registration, mut rx) = self.register_control((0, 0, corr), false)?;
        let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body)
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        let frame = self
            .control_exchange(
                &runtime,
                registration,
                &mut rx,
                frame,
                super::RESPONSE_TIMEOUT,
            )
            .await?;
        match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
            Ok(ClientControlResponse::CatalogList {
                generation,
                modules,
                ..
            }) => Ok((runtime.conn, generation, modules)),
            Ok(other) => Err(ConnError::Protocol(format!(
                "unexpected catalog.list reply: {other:?}"
            ))),
            Err(e) => Err(ConnError::Protocol(format!("decode catalog.list: {e}"))),
        }
    }

    /// The socket new requests go to now.
    fn current_conn(&self) -> ConnToken {
        self.shared.current.lock().unwrap().runtime.conn
    }

    /// The catalog this connection serves now: the generation its watch on the current socket
    /// last reported. `None` when it can't tell (it doesn't follow the catalog, or no watch on
    /// the current socket has reported yet).
    pub(crate) fn catalog_generation(&self) -> Option<CatalogGeneration> {
        let memo = &self.shared.catalog;
        if !memo.following.load(Ordering::Acquire) {
            return None;
        }
        let current = self.current_conn();
        let pushed = memo.state.lock().unwrap().pushed;
        pushed
            .filter(|(on, _)| *on == current)
            .map(|(conn, generation)| CatalogGeneration { conn, generation })
    }
}

#[cfg(test)]
#[path = "catalog_watch_tests.rs"]
mod tests;
