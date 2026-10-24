//! Installing the provider-held runtime in the daemon.
//!
//! A provider that declares `late_results` may answer a tool call with a `held` marker and
//! settle it later. Three things make that safe in this process:
//!
//! - **Routing by the frozen plan.** The tool plane reads each provider's result format and
//!   `late_results` declaration from the session's frozen plan manifest, never from a live
//!   catalog, so a provider whose catalog changes after admission cannot change how a resumed
//!   run reads a reply.
//! - **Registration before send, delivery by the batch.** Every call whose key goes out is
//!   registered with the intake first. Its terminal is written by the run's own batch, which is
//!   the only writer; the puller only hands the entry over.
//! - **One puller per provider module.** A module's feed runs only while a registered call on it
//!   is held, owes its batch commit, or an entry is still unacknowledged. A module named only by
//!   ordinary calls never starts one.
//!
//! The `held-reply/v1` and `content/v1` route capabilities stay UNDECLARED until cancel and stop
//! can close out a held call: until then a provider never sees that this build accepts `held`, so
//! no production call can enter a hold. The tests install a plane that declares both, so the
//! whole path — held reply, durable pause, pulled result, ack, resumed run — is exercised.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use broca_core::tool_plane::LateResultRegistration;
use broca_module::late_results_puller::{
    FeedError, FeedReply, HeldDelivery, LateResultsFeed, LateResultsIntake,
};
use broca_module::late_results_store::LateResultsStore;
use broca_protocol::BindIdentity;
use cortexkit_role_tool_provider::late_results::{Cursor, LateEntry};

/// The intake store's file name under the state root, kept apart from `run-index.db` the way the
/// other projections are.
pub const LATE_RESULTS_DB: &str = "late-results.db";

/// Where a handed-over entry must land: the single writer for that call is the run's batch.
#[async_trait::async_trait]
pub trait HeldEntrySink: Send + Sync {
    /// Hand one entry to the call's run's batch. `Ok(())` means its terminal is durable.
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        entry: &LateEntry,
    ) -> Result<(), String>;
}

/// One route per provider module, opened on the consumer-identity connection.
pub struct FeedRoutes {
    conn: Arc<broca_subc::SubcConnection>,
    identity: BindIdentity,
    feeds: Mutex<HashMap<String, Arc<broca_subc::LateResultsFeed>>>,
}

impl FeedRoutes {
    /// `identity` names each feed route; the custodian itself comes from the connection's
    /// consumer identity, so every module's feed shares this one bind.
    pub fn new(conn: Arc<broca_subc::SubcConnection>, identity: BindIdentity) -> Self {
        Self {
            conn,
            identity,
            feeds: Mutex::new(HashMap::new()),
        }
    }

    async fn feed(&self, module: &str) -> Result<Arc<broca_subc::LateResultsFeed>, FeedError> {
        if let Some(feed) = self.feeds.lock().unwrap().get(module).cloned() {
            return Ok(feed);
        }
        let feed =
            broca_subc::LateResultsFeed::open(Arc::clone(&self.conn), module, &self.identity)
                .await
                .map_err(feed_error)?;
        let feed = Arc::new(feed);
        self.feeds
            .lock()
            .unwrap()
            .insert(module.to_owned(), Arc::clone(&feed));
        Ok(feed)
    }
}

#[async_trait::async_trait]
impl LateResultsFeed for FeedRoutes {
    async fn pull(
        &self,
        module: &str,
        since: Option<&Cursor>,
        limit: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        let feed = self.feed(module).await?;
        let reply = feed.pull(since, limit).await.map_err(feed_error)?;
        Ok(FeedReply {
            entries: reply.entries,
            cursor: reply.cursor,
            more: reply.more,
        })
    }

    async fn ack(&self, module: &str, through: &Cursor) -> Result<(), FeedError> {
        let feed = self.feed(module).await?;
        feed.ack(through).await.map_err(feed_error)
    }
}

fn feed_error(error: broca_subc::LateResultsError) -> FeedError {
    if error.is_cursor_incarnation_changed() {
        return FeedError::CursorIncarnationChanged;
    }
    if error.is_permanent() {
        // An unreadable answer or a refused route will not improve by waiting: the puller
        // backs off for good and an ack-only feed is dropped, instead of retrying forever.
        return FeedError::Abandoned(error.to_string());
    }
    FeedError::Retryable(error.to_string())
}

/// The delivery path from the puller to the session's actor.
pub struct ActorDelivery {
    send: Arc<DeliveryPost>,
    timeout: std::time::Duration,
}

/// Posts one delivery request and answers through the channel when the batch settles it.
type DeliveryPost = dyn Fn(LateResultRegistration, LateEntry, tokio::sync::oneshot::Sender<Result<(), String>>)
    + Send
    + Sync;

impl ActorDelivery {
    /// `send` posts one delivery request for a registration's session and answers through the
    /// channel once the run's batch has synced the terminal (or refused it).
    pub fn new(
        send: impl Fn(LateResultRegistration, LateEntry, tokio::sync::oneshot::Sender<Result<(), String>>)
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            send: Arc::new(send),
            timeout: std::time::Duration::from_secs(60),
        }
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait::async_trait]
impl HeldDelivery for ActorDelivery {
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        entry: &LateEntry,
    ) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (self.send)(registration.clone(), entry.clone(), tx);
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("the session stopped before settling the delivery".into()),
            Err(_) => Err("the session did not settle the delivery in time".into()),
        }
    }
}

/// Delivers a pulled entry back to the provider that holds the call, on behalf of the run.
///
/// The provider's answer becomes the call's result only through this post: the run's batch is
/// still the single writer of the terminal, and it receives the body as a completed reply on
/// the route, under the same call key, that the held call was sent on.
pub struct ProviderDelivery {
    #[allow(dead_code)]
    plane: Arc<broca_subc::SubcToolPlane>,
    #[allow(dead_code)]
    connection: Arc<broca_subc::SubcConnection>,
    #[allow(dead_code)]
    identity: BindIdentity,
}

impl ProviderDelivery {
    /// `connection` must be the consumer-identity connection the plane dispatches on: a held
    /// call's result is served only to its custodian.
    pub fn new(
        plane: Arc<broca_subc::SubcToolPlane>,
        connection: Arc<broca_subc::SubcConnection>,
        identity: BindIdentity,
    ) -> Self {
        Self {
            plane,
            connection,
            identity,
        }
    }
}

#[async_trait::async_trait]
impl HeldEntrySink for ProviderDelivery {
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        entry: &LateEntry,
    ) -> Result<(), String> {
        Err(format!(
            "provider delivery is not wired yet for {} ({:?})",
            registration.call_key,
            entry.kind()
        ))
    }
}

/// The intake a tool plane is handed, with the store kept so boot recovery can read it.
pub struct InstalledIntake {
    pub intake: Arc<LateResultsIntake>,
    pub store: Arc<LateResultsStore>,
}

/// Build the intake for this process.
///
/// The store is created beside every other projection under the state root. It is not rebuilt
/// here: a store this process creates is incomplete, and the intake shows that to every puller by
/// starting each ack cursor from the beginning of the provider's incarnation.
pub fn build_intake(
    state_root: &std::path::Path,
    wal: Arc<broca_core::FileWalStore>,
    consumer_module: impl Into<String>,
    delivery: Arc<dyn HeldDelivery>,
    feed: Arc<dyn LateResultsFeed>,
) -> Result<InstalledIntake, String> {
    let store = Arc::new(LateResultsStore::open(&state_root.join(LATE_RESULTS_DB))?);
    let intake = LateResultsIntake::new(Arc::clone(&store), wal, consumer_module, delivery, feed);
    Ok(InstalledIntake { intake, store })
}
