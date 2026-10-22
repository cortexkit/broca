//! Reading a scope's current state from the subc daemon (`scope.describe`).
//!
//! The tool plane asks when a route it opened under a scope has closed, to learn whether the
//! scope (or its parent) is gone or still live, and under which stamp. The daemon answers
//! `scope.describe` only on a connection that holds a module registration, so the question goes
//! over broca's MODULE connection (the one its serve loop registered with HELLO), not over the
//! plane's consumer connection. [`ModuleScopeDescriber`] is that path: the serve loop hands it
//! the connection's outbound frame queue and passes it every channel-0 reply it reads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use subc_protocol::scope::{ScopeStamp, ScopeStatus};
use subc_protocol::session::{ModuleControlRequestFromModule, ModuleControlResponseToModule};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Principal, Priority};
use tokio::sync::{mpsc, oneshot};

/// What `scope.describe` said about one `(owner, ref)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedScope {
    pub status: ScopeStatus,
    /// The live epoch, or for an ended scope the most recent epoch that ended.
    pub scope_epoch: Option<u64>,
    /// Whether the owner has synced since this daemon incarnation started.
    pub owner_synced: bool,
    /// Whether the owner is a module in the daemon's supervised roster.
    pub owner_configured: bool,
    /// The stamp the daemon would put on a bind now; present only for a live scope.
    pub stamp: Option<ScopeStamp>,
}

/// Asks the daemon for one scope's current state. `Err` means the question could not be
/// answered (no module connection, a refusal, a timeout); the caller treats that as "can't
/// tell", never as either answer.
#[async_trait]
pub trait ScopeDescriber: Send + Sync {
    async fn describe(&self, owner: &Principal, scope_ref: &str) -> Result<DescribedScope, String>;
}

/// How long one `scope.describe` waits for its reply. The daemon answers from memory, so a
/// reply that takes longer means the connection is in trouble, and the caller should hear
/// "can't tell" rather than wait on it.
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The first correlation id this describer uses. Far above anything the serve loop uses for
/// its own frames (HELLO is corr 1), so a reply to one of the serve loop's frames is never
/// taken for a describe reply.
const FIRST_DESCRIBE_CORR: u64 = 1 << 40;

/// `scope.describe` over a registered module connection.
///
/// The connection belongs to the serve loop: it [`attach`](Self::attach)es its outbound queue
/// once registered, hands every channel-0 frame it reads to [`on_frame`](Self::on_frame), and
/// [`detach`](Self::detach)es when the connection ends. Between those, and while detached,
/// `describe` answers `Err`.
pub struct ModuleScopeDescriber {
    writer: Mutex<Option<broca_diagnostics::Sender<Frame>>>,
    registered: tokio::sync::watch::Sender<bool>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    next_corr: AtomicU64,
}

struct PendingDescribe<'a> {
    describer: &'a ModuleScopeDescriber,
    corr: u64,
}

impl Drop for PendingDescribe<'_> {
    fn drop(&mut self) {
        self.describer.pending.lock().unwrap().remove(&self.corr);
    }
}

impl Default for ModuleScopeDescriber {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleScopeDescriber {
    pub fn new() -> Self {
        Self {
            writer: Mutex::new(None),
            registered: tokio::sync::watch::channel(false).0,
            pending: Mutex::new(HashMap::new()),
            next_corr: AtomicU64::new(FIRST_DESCRIBE_CORR),
        }
    }

    /// Start sending on a registered module connection's outbound frame queue.
    pub fn attach(&self, writer: mpsc::Sender<Frame>) {
        self.attach_observed(writer.into());
    }

    /// Include module-originated scope requests in the connection's egress counters.
    pub fn attach_observed(&self, writer: broca_diagnostics::Sender<Frame>) {
        *self.writer.lock().unwrap() = Some(writer);
        self.registered.send_replace(true);
    }

    /// Wait until HELLO_ACK has registered the serve connection and attached its routes.
    pub async fn wait_registered(&self) {
        let mut registered = self.registered.subscribe();
        while !*registered.borrow_and_update() {
            if registered.changed().await.is_err() {
                return;
            }
        }
    }

    /// Stop: the connection ended. Every describe still waiting fails at once.
    pub fn detach(&self) {
        self.writer.lock().unwrap().take();
        self.registered.send_replace(false);
        self.pending.lock().unwrap().clear();
    }

    /// Offer a frame the module connection read. Returns true when it was the reply to one of
    /// this describer's requests (the caller then has nothing more to do with it).
    pub fn on_frame(&self, frame: &Frame) -> bool {
        if frame.header.channel != 0
            || !matches!(frame.header.ty, FrameType::Response | FrameType::Error)
        {
            return false;
        }
        let Some(waiter) = self.pending.lock().unwrap().remove(&frame.header.corr) else {
            return false;
        };
        let _ = waiter.send(frame.clone());
        true
    }
}

#[async_trait]
impl ScopeDescriber for ModuleScopeDescriber {
    async fn describe(&self, owner: &Principal, scope_ref: &str) -> Result<DescribedScope, String> {
        let writer = self
            .writer
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "broca's module connection is not registered".to_owned())?;
        let body = serde_json::to_vec(&ModuleControlRequestFromModule::ScopeDescribe {
            owner: owner.clone(),
            scope_ref: scope_ref.to_owned(),
        })
        .map_err(|error| error.to_string())?;
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            0,
            0,
            corr,
            body,
        )
        .map_err(|error| error.to_string())?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(corr, tx);
        // Cancellation can happen at either await, including before writer capacity exists.
        let _pending = PendingDescribe {
            describer: self,
            corr,
        };
        let reply = match tokio::time::timeout(DESCRIBE_TIMEOUT, async {
            writer.send(frame).await.map_err(|_| ())?;
            rx.await.map_err(|_| ())
        })
        .await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => return Err("broca's module connection closed".into()),
            Err(_) => {
                return Err(format!(
                    "scope.describe could not send and receive a reply within {DESCRIBE_TIMEOUT:?}"
                ));
            }
        };
        decode_reply(&reply)
    }
}

fn decode_reply(reply: &Frame) -> Result<DescribedScope, String> {
    if reply.header.ty == FrameType::Error {
        return Err(match serde_json::from_slice::<ErrorBody>(&reply.body) {
            Ok(error) => format!("{} — {}", error.code, error.message),
            Err(_) => String::from_utf8_lossy(&reply.body).into_owned(),
        });
    }
    match serde_json::from_slice::<ModuleControlResponseToModule>(&reply.body) {
        Ok(ModuleControlResponseToModule::ScopeDescribe {
            status,
            scope_epoch,
            owner_synced,
            owner_configured,
            scope,
            ..
        }) => Ok(DescribedScope {
            status,
            scope_epoch,
            owner_synced,
            owner_configured,
            stamp: scope,
        }),
        Ok(other) => Err(format!("unexpected reply to scope.describe: {other:?}")),
        Err(error) => Err(format!("decode scope.describe reply: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn owner() -> Principal {
        Principal::Reserved {
            module_id: "owner".into(),
        }
    }

    fn queued_frame() -> Frame {
        Frame::build(
            FrameType::Ping,
            Flags::new(false, Priority::Interactive, false),
            0,
            0,
            1,
            Vec::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn cancelled_describe_cleans_pending_before_and_after_enqueue() {
        for full in [true, false] {
            let describer = ModuleScopeDescriber::new();
            let (writer, mut reader) = mpsc::channel(1);
            if full {
                writer.try_send(queued_frame()).unwrap();
            }
            describer.attach(writer);
            let owner = owner();
            let mut request = Box::pin(describer.describe(&owner, "scope"));
            assert!(request.as_mut().now_or_never().is_none());
            assert_eq!(describer.pending.lock().unwrap().len(), 1);
            drop(request);
            assert!(describer.pending.lock().unwrap().is_empty());
            let written = reader.try_recv().unwrap();
            assert_eq!(
                written.header.ty,
                if full {
                    FrameType::Ping
                } else {
                    FrameType::Request
                }
            );
            assert!(reader.try_recv().is_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn describe_backpressure_is_inside_the_reply_deadline() {
        let describer = ModuleScopeDescriber::new();
        let (writer, mut reader) = mpsc::channel(1);
        writer.try_send(queued_frame()).unwrap();
        describer.attach(writer);
        let started = tokio::time::Instant::now();
        let error = tokio::time::timeout(
            DESCRIBE_TIMEOUT + Duration::from_secs(1),
            describer.describe(&owner(), "scope"),
        )
        .await
        .expect("writer backpressure must obey the describe deadline")
        .unwrap_err();
        assert!(error.contains("could not send and receive"), "{error}");
        assert_eq!(started.elapsed(), DESCRIBE_TIMEOUT);
        assert!(describer.pending.lock().unwrap().is_empty());
        assert_eq!(reader.try_recv().unwrap().header.ty, FrameType::Ping);
        assert!(
            reader.try_recv().is_err(),
            "a timed-out describe must not enqueue later"
        );
    }
}
