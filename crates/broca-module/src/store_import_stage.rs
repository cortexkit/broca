//! Where a head import writes the session it imports before anything can read it, and how that
//! session is published over the empty session the import was admitted into.
//!
//! An import's history may need more log batches than one, and a crash between two batches
//! would leave part of it. So the actor writes the whole import into a replacement of its empty
//! session ([`Session::stage_replacement`]) in a private staging folder, then publishes it with
//! the publication a move uses (`store_publish`): the staged log and objects are renamed beside
//! the empty session's, each synced, and the staged head replaces the live head at its next
//! version. That head write is the one point of no return. Before it the live session is still
//! the empty one, and what the attempt left is never named by any head; after it the session is
//! the whole import.
//!
//! Each attempt stages in its own folder under `<state root>/session-store-imports/staging/`,
//! outside every class root, so nothing serves or backs it up. An attempt that fails removes its
//! folder; what a crashed one left is removed when the session store next opens
//! ([`clear_import_staging`]), before any actor runs, and is never published.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend, Version};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::{Session, SessionError};
use broca_wal::migration;

use crate::registry::store_move::MoveRefusal;
use crate::store_publish::{publish_data_at, publish_head_at, PublicationDirs};

/// The folder every import attempt stages in, one folder per attempt.
pub fn import_staging_root(state_root: &Path) -> PathBuf {
    state_root.join("session-store-imports").join("staging")
}

/// Removes what every import attempt staged under `state_root`. The session store does this when
/// it opens, before any actor runs, so a crashed attempt's leftovers never pile up; no attempt
/// publishes from a folder it didn't stage itself.
pub fn clear_import_staging(state_root: &Path) -> std::io::Result<()> {
    let root = import_staging_root(state_root);
    match std::fs::remove_dir_all(&root) {
        Ok(()) => migration::sync_dir(root.parent().expect("the staging root has a parent")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Where an import stages the session it writes, and how it publishes it over the live one.
#[async_trait::async_trait]
pub trait ImportStaging<B: SessionBackend>: Send + Sync {
    /// A new, empty, private place for import attempt `attempt` (a name no other attempt uses),
    /// as a backend its session is created in.
    fn begin(&self, attempt: &str) -> Result<B, String>;

    /// Publishes the session `attempt` staged at `addr` over the live session there, whose head
    /// is at version `live`. Once this returns `Ok` the live session is the staged one. An
    /// error may come after the head was replaced, so the caller must load the session again
    /// to know which it is.
    async fn publish(&self, attempt: &str, addr: &SessionAddr, live: Version)
        -> Result<(), String>;

    /// Removes whatever `attempt` staged that is still in its private place.
    fn discard(&self, attempt: &str);

    /// Loads the live session at `addr` for writing.
    async fn load(
        &self,
        addr: SessionAddr,
        triple: &SessionTriple,
    ) -> Result<Session<B>, SessionError>;
}

/// The staging of the session store's class roots: a folder per attempt under the state root,
/// published into the root of the session's class.
pub struct FsImportStaging {
    state_root: PathBuf,
    /// The class root the live session is in.
    store: PathBuf,
    deployment: Arc<DeploymentKey>,
    directories: Mutex<PublicationDirs>,
    /// Where a test stops the publication as a crash would.
    #[cfg(test)]
    pub(crate) cut: Option<ImportCut>,
}

impl FsImportStaging {
    pub fn new(state_root: &Path, store: PathBuf, deployment: Arc<DeploymentKey>) -> Self {
        Self {
            state_root: state_root.to_path_buf(),
            store,
            deployment,
            directories: Mutex::default(),
            #[cfg(test)]
            cut: None,
        }
    }

    fn staging(&self, attempt: &str) -> PathBuf {
        import_staging_root(&self.state_root).join(attempt)
    }
}

/// Why a publication stopped.
enum Stop {
    Refused(MoveRefusal),
    /// A test's crash: nothing after it runs.
    #[cfg(test)]
    Crashed,
}

impl From<MoveRefusal> for Stop {
    fn from(refusal: MoveRefusal) -> Self {
        Self::Refused(refusal)
    }
}

impl Stop {
    fn message(self) -> String {
        match self {
            Self::Refused(refusal) => refusal.message,
            #[cfg(test)]
            Self::Crashed => "crashed".into(),
        }
    }
}

#[async_trait::async_trait]
impl ImportStaging<LocalBackend> for FsImportStaging {
    fn begin(&self, attempt: &str) -> Result<LocalBackend, String> {
        let staging = self.staging(attempt);
        broca_owner_only::create_dir_all(&staging)
            .map_err(|error| format!("the import's staging folder couldn't be created: {error}"))?;
        Ok(LocalBackend::new(staging))
    }

    async fn publish(
        &self,
        attempt: &str,
        addr: &SessionAddr,
        live: Version,
    ) -> Result<(), String> {
        let staging = self.staging(attempt);
        let data = publish_data_at::<Stop>(
            &self.state_root,
            &staging,
            &self.store,
            &addr.0,
            &mut self.directories.lock().unwrap_or_else(|p| p.into_inner()),
            |_boundary| {
                #[cfg(test)]
                if self.cut.as_ref().is_some_and(|cut| cut.at == _boundary) {
                    return Err(Stop::Crashed);
                }
                Ok(())
            },
        );
        let published = match data {
            Ok(()) => publish_head_at::<Stop>(&staging, &self.store, &addr.0, Some(live))
                .await
                .map(|_| ()),
            Err(stop) => Err(stop),
        };
        match published {
            Ok(()) => Ok(()),
            #[cfg(test)]
            Err(Stop::Crashed) => {
                let cut = self.cut.as_ref().expect("a crash comes from a cut");
                cut.hold(cut.at).await;
                unreachable!("a cut holds forever")
            }
            Err(stop) => Err(stop.message()),
        }
    }

    fn discard(&self, attempt: &str) {
        let staging = self.staging(attempt);
        match std::fs::remove_dir_all(&staging) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            // Left for the next open of the session store to remove.
            Err(error) => tracing::warn!(
                target: "store_import",
                attempt,
                "an import's staging folder couldn't be removed: {error}"
            ),
        }
    }

    async fn load(
        &self,
        addr: SessionAddr,
        triple: &SessionTriple,
    ) -> Result<Session<LocalBackend>, SessionError> {
        Session::load(
            LocalBackend::new(&self.store),
            addr,
            triple,
            &self.deployment,
        )
        .await
    }
}

/// Where a test stops an import as a crash would: when the import reaches the step named `at`,
/// `reached` is notified and the import never goes on. The test then aborts the actor's task,
/// so nothing after that step runs, as when the process dies there.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ImportCut {
    pub at: &'static str,
    pub reached: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl ImportCut {
    pub(crate) fn new(at: &'static str) -> Self {
        Self {
            at,
            reached: Arc::default(),
        }
    }

    /// Holds forever once `step` is the cut's.
    pub(crate) async fn hold(&self, step: &'static str) {
        if step == self.at {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

/// A staging for sessions in memory, for tests: an attempt stages in a backend of its own, and
/// publishing copies its logs and objects into the live backend, then replaces the live head at
/// its next version.
#[cfg(test)]
pub(crate) struct MemoryImportStaging {
    live: &'static broca_session_store::backend::MemoryBackend,
    deployment: DeploymentKey,
    attempts: Mutex<
        std::collections::HashMap<String, &'static broca_session_store::backend::MemoryBackend>,
    >,
}

#[cfg(test)]
impl MemoryImportStaging {
    pub(crate) fn new(
        live: &'static broca_session_store::backend::MemoryBackend,
        deployment: DeploymentKey,
    ) -> Self {
        Self {
            live,
            deployment,
            attempts: Mutex::default(),
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl ImportStaging<&'static broca_session_store::backend::MemoryBackend> for MemoryImportStaging {
    fn begin(
        &self,
        attempt: &str,
    ) -> Result<&'static broca_session_store::backend::MemoryBackend, String> {
        let staged: &'static broca_session_store::backend::MemoryBackend =
            Box::leak(Box::default());
        self.attempts
            .lock()
            .unwrap()
            .insert(attempt.to_owned(), staged);
        Ok(staged)
    }

    async fn publish(
        &self,
        attempt: &str,
        addr: &SessionAddr,
        live: Version,
    ) -> Result<(), String> {
        let staged = *self
            .attempts
            .lock()
            .unwrap()
            .get(attempt)
            .ok_or("no such attempt")?;
        let failed = |error: broca_session_store::backend::BackendError| format!("{error:?}");
        for log in staged.log_names(addr) {
            let end = staged.log_end(addr, &log).await.map_err(failed)?;
            let bytes = staged.read_log(addr, &log, 0..end).await.map_err(failed)?;
            self.live.set_log(addr, &log, bytes);
        }
        for name in staged.object_names(addr) {
            let bytes = staged
                .get_object(addr, &name)
                .await
                .map_err(failed)?
                .ok_or("a staged object is missing")?;
            self.live.set_object(addr, &name, bytes);
        }
        let (head, _) = staged
            .read_head(addr)
            .await
            .map_err(failed)?
            .ok_or("the staged head is missing")?;
        self.live
            .write_head(addr, &head, Some(live), live + 1)
            .await
            .map_err(failed)?;
        Ok(())
    }

    fn discard(&self, attempt: &str) {
        self.attempts.lock().unwrap().remove(attempt);
    }

    async fn load(
        &self,
        addr: SessionAddr,
        triple: &SessionTriple,
    ) -> Result<Session<&'static broca_session_store::backend::MemoryBackend>, SessionError> {
        Session::load(self.live, addr, triple, &self.deployment).await
    }
}
