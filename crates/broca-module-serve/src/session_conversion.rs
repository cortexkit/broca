//! Opt-in cutover of legacy logs through the existing converter and move verifier.
//! Source files are retained outside the state root. Only the destination head publishes a
//! session; the temporary ledger records intents before filesystem operations begin.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime};

use broca_core::media_store::MediaBytes;
use broca_protocol::BindIdentity;
use broca_session_convert::{
    preconditions,
    stage::{Stage, StageError},
    verify::{MediaRoot, Mode, Verify},
};
use broca_session_store::keys::DeploymentKey;
use broca_wal::{FramedRecord, StorageClass, Wal, WalLineageId, WalRecord};
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::conversion_ledger::{Ledger, Progress, Row, Source};

pub(crate) mod parallel;

/// Cutover-only switches, removed with legacy storage. Deploying the converter alone does
/// not open sources or move data; archives and the live startup window are enabled separately.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Cutover-only archive switch, enabled by the operator after a clone has no unexpected failures.
    pub archives: bool,
    /// Cutover-only startup maintenance window switch, off until the cutover deploy.
    pub live_at_startup: bool,
    pub backup_root: Option<PathBuf>,
}

impl Config {
    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct File {
            #[serde(default)]
            session_conversion: Config,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        serde_json::from_str::<File>(&text)
            .map(|file| file.session_conversion)
            .map_err(|error| format!("parse session_conversion: {error}"))
    }

    pub fn load_default() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Self::default())
                    }
                    Err(error) => Err(format!("read session_conversion: {error}")),
                }
            }
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.archives || self.live_at_startup
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Health {
    pub live_converting: bool,
    pub live_background_converted: u64,
    pub live_skipped_busy: u64,
    pub live_remaining: u64,
    pub deferred_no_identity: u64,
    pub progress: Progress,
    #[serde(skip)]
    pub failed_sessions: BTreeMap<String, String>,
    pub error: Option<String>,
}

static HEALTH: OnceLock<Arc<RwLock<Health>>> = OnceLock::new();

pub fn health() -> Health {
    HEALTH
        .get()
        .map(|health| health.read().unwrap_or_else(|p| p.into_inner()).clone())
        .unwrap_or_default()
}

/// The global startup barrier precedes every routed session operation. Permanent per-session
/// failures outlive the barrier, so one damaged or unsupported session cannot stop other users.
fn refusal_of(health: &Health, session: &BindIdentity) -> Option<(bool, String)> {
    if health.live_converting {
        return Some((
            true,
            "live logs are converting before session service starts".into(),
        ));
    }
    health
        .failed_sessions
        .get(&broca_core::session_addr(session))
        .map(|reason| (false, reason.clone()))
}

#[derive(Clone)]
pub struct Engine {
    root: PathBuf,
    backup: PathBuf,
    pub ledger: Arc<Ledger>,
    health: Arc<RwLock<Health>>,
    config: Config,
    stderr_warnings: bool,
    source_index: Option<Arc<broca_store::SqliteStore>>,
    inventory: Option<Arc<broca_store::session_inventory::SessionInventory>>,
    mover: Arc<OnceLock<Arc<crate::store_move::StoreMove>>>,
    archive_inventory: Arc<Mutex<ArchiveInventory>>,
    directories: Arc<Mutex<crate::store_move::PublicationDirs>>,
    pass: Arc<tokio::sync::Mutex<()>>,
    parallelism: usize,
    #[cfg(test)]
    pub(crate) crash: Option<&'static str>,
    #[cfg(test)]
    pub(crate) faults: Arc<crate::store_move::Faults>,
    #[cfg(test)]
    pub(crate) background_fenced: Option<Arc<BackgroundProbe>>,
    #[cfg(test)]
    pub(crate) verification_records: Option<fn(&mut Vec<FramedRecord>)>,
    #[cfg(test)]
    pub(crate) parallel_probe: Option<Arc<parallel::Probe>>,
}

#[cfg(test)]
pub(crate) struct BackgroundProbe {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Semaphore,
    pub sessions: Mutex<Vec<String>>,
}

#[derive(Default)]
struct ArchiveInventory {
    timestamps: Vec<Option<SystemTime>>,
    paths: Arc<Vec<PathBuf>>,
}

#[derive(Debug)]
struct Failure {
    class: String,
    terminal: bool,
    message: String,
}

impl Failure {
    fn failed(class: &str, message: impl ToString) -> Self {
        Self {
            class: class.into(),
            terminal: false,
            message: message.to_string(),
        }
    }
    fn refused(class: &str, message: impl ToString) -> Self {
        Self {
            class: class.into(),
            terminal: true,
            message: message.to_string(),
        }
    }
}

fn io(error: impl ToString) -> Failure {
    Failure::failed("io_failed", error)
}

fn class(source: &Source) -> Result<StorageClass, Failure> {
    StorageClass::ALL
        .into_iter()
        .find(|class| class.name() == source.class)
        .ok_or_else(|| io("unknown conversion storage class"))
}

impl Engine {
    /// Disabled configurations do not even open the ledger or inspect a source directory.
    pub fn configured(root: &Path, config: Config) -> Result<Option<Self>, String> {
        if !config.enabled() {
            return Ok(None);
        }
        let root = root.canonicalize().map_err(|error| error.to_string())?;
        let backup = config.backup_root.clone().unwrap_or_else(|| {
            root.parent()
                .expect("state root has a parent")
                .join("broca-converted-sources")
        });
        broca_owner_only::create_dir_all(&backup).map_err(|error| error.to_string())?;
        let backup = backup.canonicalize().map_err(|error| error.to_string())?;
        if backup.starts_with(&root) || root.starts_with(&backup) {
            return Err(
                "converted source backup must be outside, and not contain, the state root".into(),
            );
        }
        broca_wal::migration::sync_dir(backup.parent().ok_or("backup has no parent")?)
            .map_err(|error| error.to_string())?;
        let ledger = Arc::new(Ledger::open(&root)?);
        let mut health = Health {
            live_converting: config.live_at_startup,
            progress: ledger.progress()?,
            ..Health::default()
        };
        for row in ledger.rows()? {
            if row.source.member.is_none() && row.step != "done" {
                health.live_remaining += 1;
            }
            if !Path::new(&row.source.backup).starts_with(&backup) {
                return Err(
                    "conversion ledger names a backup outside the configured backup root".into(),
                );
            }
            if Path::new(&row.source.path)
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err("conversion ledger source is not a state-root-relative path".into());
            }
            if matches!(row.outcome.as_deref(), Some("failed" | "refused")) {
                if let Some(address) = source_address(&row.source) {
                    health.failed_sessions.insert(
                        address,
                        row.refusal.unwrap_or_else(|| "conversion_failed".into()),
                    );
                }
            }
        }
        let index = root.join("run-index.db");
        let source_index = if index.try_exists().map_err(|error| error.to_string())? {
            Some(Arc::new(
                broca_store::SqliteStore::open_read_only(&index)
                    .map_err(|error| error.to_string())?,
            ))
        } else {
            None
        };
        let inventory = source_index
            .as_ref()
            .map(|_| broca_store::session_inventory::SessionInventory::open(&index))
            .transpose()
            .map_err(|error| error.to_string())?
            .map(Arc::new);
        Ok(Some(Self {
            root,
            backup,
            ledger,
            health: Arc::new(RwLock::new(health)),
            config,
            stderr_warnings: false,
            source_index,
            mover: Arc::default(),
            archive_inventory: Arc::default(),
            directories: Arc::default(),
            pass: Arc::default(),
            parallelism: parallel::DAEMON_WORKERS,
            inventory,
            #[cfg(test)]
            crash: None,
            #[cfg(test)]
            faults: Default::default(),
            #[cfg(test)]
            background_fenced: None,
            #[cfg(test)]
            verification_records: None,
            #[cfg(test)]
            parallel_probe: None,
        }))
    }

    pub fn publish_health(&self) {
        let _ = HEALTH.set(Arc::clone(&self.health));
    }

    #[cfg(test)]
    pub(crate) fn health_for_test(&self) -> Health {
        self.health
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn set_parallelism_for_test(&mut self, workers: usize) {
        assert!(workers > 0);
        self.parallelism = workers;
    }

    /// Reuse the online move's checks for unsettled runs and held calls, and remove today's
    /// cached session views and late-result routing after the new storage takes ownership.
    pub fn set_mover(&self, mover: Arc<crate::store_move::StoreMove>) {
        let _ = self.mover.set(mover);
    }

    fn newest_archive(
        &self,
        address: &str,
    ) -> Result<Option<broca_wal::archive::ArchiveMember>, Failure> {
        let folders = [
            self.root.join("wal-archive"),
            self.root.join("wal-archive-durable"),
        ];
        let timestamps = folders
            .iter()
            .map(|folder| match std::fs::metadata(folder) {
                Ok(metadata) => metadata.modified().map(Some).map_err(io),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(io(error)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let paths = {
            let mut inventory = self.archive_inventory.lock().map_err(io)?;
            if inventory.timestamps != timestamps {
                let mut paths = Vec::new();
                for folder in &folders {
                    paths.extend(files(folder, "ark").map_err(io)?);
                }
                paths.sort_by_key(|path| {
                    std::cmp::Reverse(broca_wal::archive::fold_timestamp(path).unwrap_or_default())
                });
                inventory.paths = Arc::new(paths);
                inventory.timestamps = timestamps;
            }
            Arc::clone(&inventory.paths)
        };
        broca_wal::archive::read_newest_member(&paths, address, broca_wal::DEFAULT_MAX_READ_BYTES)
            .map_err(io)
    }

    pub fn install_guard(&self, registry: &broca_module::LineageRegistry) {
        let health = Arc::clone(&self.health);
        registry.set_conversion_guard(Arc::new(move |session| {
            refusal_of(&health.read().unwrap_or_else(|p| p.into_inner()), session)
        }));
    }

    pub fn record_error(&self, error: String) {
        tracing::error!(target: "session_conversion", "{error}");
        self.health.write().unwrap_or_else(|p| p.into_inner()).error = Some(error);
    }

    fn step(
        &self,
        source: &Source,
        step: &str,
        outcome: Option<&str>,
        refusal: Option<&str>,
    ) -> Result<(), Failure> {
        self.step_with_detail(source, step, outcome, refusal, None)
    }

    fn step_with_detail(
        &self,
        source: &Source,
        step: &str,
        outcome: Option<&str>,
        refusal: Option<&str>,
        detail: Option<&str>,
    ) -> Result<(), Failure> {
        let old = self
            .ledger
            .row(&source.id)
            .map_err(io)?
            .ok_or_else(|| io("conversion source is not discovered"))?;
        self.ledger
            .advance(
                &source.id,
                step,
                outcome,
                refusal,
                detail.or_else(|| {
                    (old.outcome.as_deref() == outcome && old.refusal.as_deref() == refusal)
                        .then_some(old.detail.as_deref())
                        .flatten()
                }),
            )
            .map_err(io)?;
        let mut health = self.health.write().unwrap_or_else(|p| p.into_inner());
        change_count(&mut health.progress.steps, &old.step, -1);
        change_count(&mut health.progress.steps, step, 1);
        if let Some(old) = old.outcome {
            change_count(&mut health.progress.outcomes, &old, -1);
        }
        if let Some(outcome) = outcome {
            change_count(&mut health.progress.outcomes, outcome, 1);
        }
        if let Some(old) = old.refusal {
            change_count(&mut health.progress.refusals, &old, -1);
        }
        if let Some(refusal) = refusal {
            change_count(&mut health.progress.refusals, refusal, 1);
        }
        drop(health);
        #[cfg(test)]
        if self.crash == Some(step)
            || self
                .parallel_probe
                .as_ref()
                .is_some_and(|probe| probe.crash(source, step))
        {
            return Err(Failure::failed("crash", step));
        }
        Ok(())
    }

    fn discover(&self, source: &Source) -> Result<(), String> {
        if self.ledger.row(&source.id)?.is_none() {
            self.ledger.discover(source)?;
            let mut health = self.health.write().unwrap_or_else(|p| p.into_inner());
            health.progress.sources += 1;
            change_count(&mut health.progress.steps, "discovered", 1);
        }
        Ok(())
    }

    fn source(
        &self,
        path: &Path,
        member: Option<String>,
        class: StorageClass,
    ) -> Result<Source, String> {
        let relative = path
            .strip_prefix(&self.root)
            .map_err(|error| error.to_string())?;
        let path = relative
            .to_str()
            .ok_or("conversion source path is not UTF-8")?
            .to_owned();
        Ok(Source {
            id: member
                .as_ref()
                .map(|member| format!("{path}#{member}"))
                .unwrap_or_else(|| path.clone()),
            backup: self.backup.join(relative).to_string_lossy().into_owned(),
            destination: class.store_dir().into(),
            path,
            member,
            class: class.name().into(),
        })
    }

    fn enumerate_live(&self) -> Result<Vec<Source>, String> {
        let mut sources = Vec::new();
        for (folder, class) in [
            ("wal", StorageClass::Ordinary),
            ("wal-durable", StorageClass::Durable),
            ("wal-ephemeral", StorageClass::Ephemeral),
        ] {
            for path in files(&self.root.join(folder), "wal")? {
                sources.push(self.source(&path, None, class)?);
            }
        }
        sources.sort_by(|a, b| a.id.cmp(&b.id));
        for source in &sources {
            self.discover(source)?;
        }
        // An intent survives even if the source was renamed before the ledger's final commit.
        let discovered: std::collections::BTreeSet<_> =
            sources.iter().map(|source| source.id.clone()).collect();
        for row in self.ledger.rows()? {
            if row.source.member.is_none()
                && row.outcome.as_deref() != Some("failed")
                && row.step != "done"
                && !discovered.contains(&row.source.id)
            {
                sources.push(row.source);
            }
        }
        Ok(sources)
    }

    fn enumerate_archives(&self) -> Result<Vec<Source>, String> {
        let mut sources = Vec::new();
        for (folder, class) in [
            ("wal-archive", StorageClass::Ordinary),
            ("wal-archive-durable", StorageClass::Durable),
        ] {
            let mut paths = files(&self.root.join(folder), "ark")?;
            paths.sort_by_key(|path| {
                std::cmp::Reverse(broca_wal::archive::fold_timestamp(path).unwrap_or_default())
            });
            for path in paths {
                let mut reader = broca_wal::archive::ArchiveReader::open(
                    &path,
                    broca_wal::DEFAULT_MAX_READ_BYTES,
                )
                .map_err(|error| format!("open {}: {error}", path.display()))?;
                if reader.state().map_err(|error| error.to_string())?
                    != broca_wal::archive::ContainerState::Sealed
                {
                    continue;
                }
                let members = match reader
                    .verified_members()
                    .map_err(|error| error.to_string())?
                {
                    Some(members) => members
                        .into_iter()
                        .map(|member| member.session_addr)
                        .collect::<Vec<_>>(),
                    None => reader
                        .rescan()
                        .map_err(|error| error.to_string())?
                        .into_iter()
                        .map(|member| member.session_addr)
                        .collect(),
                };
                for member in members {
                    sources.push(self.source(&path, Some(member), class)?);
                }
            }
        }
        for source in &sources {
            self.discover(source)?;
        }
        let discovered: std::collections::BTreeSet<_> =
            sources.iter().map(|source| source.id.clone()).collect();
        for row in self.ledger.rows()? {
            if row.source.member.is_some()
                && row.step != "done"
                && row.outcome.as_deref() != Some("failed")
                && !discovered.contains(&row.source.id)
            {
                sources.push(row.source);
            }
        }
        Ok(sources)
    }
}

fn files(directory: &Path, extension: &str) -> Result<Vec<PathBuf>, String> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("list {}: {error}", directory.display())),
    };
    entries
        .filter_map(|entry| match entry {
            Ok(entry) if entry.path().extension().is_some_and(|ext| ext == extension) => {
                Some(Ok(entry.path()))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error.to_string())),
        })
        .collect()
}

fn source_address(source: &Source) -> Option<String> {
    source.member.clone().or_else(|| {
        Path::new(&source.path)
            .file_stem()
            .map(|name| name.to_string_lossy().into_owned())
    })
}

fn change_count(counts: &mut BTreeMap<String, u64>, name: &str, delta: i8) {
    let count = counts.entry(name.into()).or_default();
    if delta > 0 {
        *count += 1;
    } else {
        *count = count.saturating_sub(1);
    }
    if *count == 0 {
        counts.remove(name);
    }
}

impl Engine {
    fn initialize_directories(&self) -> Result<(), String> {
        let mut directories = self.directories.lock().unwrap_or_else(|p| p.into_inner());
        for class in StorageClass::ALL {
            directories
                .ensure(&self.root, &class.store_root(&self.root))
                .map_err(|error| error.to_string())?;
        }
        directories
            .ensure_parent(&self.root, &self.root.join("conversion-staging"))
            .map_err(|error| error.to_string())
    }

    pub async fn run_live(
        &self,
        deployment: &DeploymentKey,
        runtime: Option<&Arc<broca_module::store_open::StoreRuntime>>,
        registry: Option<&broca_module::LineageRegistry>,
    ) -> Result<(), String> {
        if !self.config.live_at_startup {
            return Ok(());
        }
        let pass = Arc::new(self.pass.clone().lock_owned().await);
        self.initialize_directories()?;
        let sources = self.enumerate_live()?;
        self.run_pass(
            sources,
            parallel::Pass {
                deployment,
                runtime,
                registry,
                background: false,
                throttle: Duration::ZERO,
                lease: pass,
            },
        )
        .await?;
        self.health
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .live_converting = false;
        Ok(())
    }

    /// One background pass: archives first, then closed idle live sessions by last write.
    /// The pause is outside every session fence so other requests can open actors between copies.
    pub async fn run_archives(
        &self,
        deployment: &DeploymentKey,
        runtime: Option<&Arc<broca_module::store_open::StoreRuntime>>,
        registry: Option<&broca_module::LineageRegistry>,
        throttle: Duration,
    ) -> Result<(), String> {
        if !self.config.archives {
            return Ok(());
        }
        let pass = Arc::new(self.pass.clone().lock_owned().await);
        self.initialize_directories()?;
        let mut sources = self.enumerate_archives()?;
        // Without a registry, there is no safe serving-session idleness check.
        if registry.is_some() {
            let mut live = self.enumerate_live()?;
            live.sort_by_cached_key(|source| {
                (
                    broca_module::head_time::file_written_ms(&self.root.join(&source.path)),
                    source.id.clone(),
                )
            });
            self.health
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .live_remaining = self
                .ledger
                .rows()?
                .iter()
                .filter(|row| row.source.member.is_none() && row.step != "done")
                .count() as u64;
            sources.extend(live);
        }
        self.run_pass(
            sources,
            parallel::Pass {
                deployment,
                runtime,
                registry,
                background: true,
                throttle,
                lease: pass,
            },
        )
        .await
    }

    /// Revisit sessions skipped while busy, without rescanning the filesystem in a tight loop.
    pub async fn run_background(
        &self,
        deployment: &DeploymentKey,
        runtime: &Arc<broca_module::store_open::StoreRuntime>,
        registry: &broca_module::LineageRegistry,
        throttle: Duration,
    ) -> Result<(), String> {
        if !self.config.archives {
            return Ok(());
        }
        let stopping = registry.stopping_signal();
        while !stopping.is_cancelled() {
            self.run_archives(deployment, Some(runtime), Some(registry), throttle)
                .await?;
            tokio::select! {
                _ = stopping.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }
        Ok(())
    }

    async fn process(
        &self,
        source: &Source,
        deployment: &DeploymentKey,
        runtime: Option<&broca_module::store_open::StoreRuntime>,
        registry: Option<&broca_module::LineageRegistry>,
        background: bool,
        archive_leased: bool,
    ) -> Result<(), String> {
        let row = self
            .ledger
            .row(&source.id)?
            .ok_or("conversion source is absent")?;
        let source = &row.source;
        if row.step == "done" {
            return Ok(());
        }
        // Container retirement needs the archive-pass lease even when this member is damaged
        // and cannot supply a session identity for the ordinary per-session fence.
        let _archive_pass = if source.member.is_some() && !archive_leased {
            Some(
                broca_core::lease::acquire_archive_pass(&self.root)
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        // Catch every poll, not just construction of the async future. With panic = "unwind"
        // in dev and release, a session panic drops its fences without unwinding its worker.
        // The panicked future is never resumed; its partial work follows the same committed
        // ledger intents and ordinary failure path as a conversion error.
        let result =
            AssertUnwindSafe(self.convert_one(&row, deployment, runtime, registry, background))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    let detail = panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("non-string panic payload");
                    Err(Failure::failed("panic", detail))
                });
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.class == "crash" => Err(error.message),
            Err(error) if background && error.class == "session_busy" => {
                self.health
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .live_skipped_busy += 1;
                Ok(())
            }
            Err(error) if background && error.class == "deferred_no_identity" => {
                self.step_with_detail(
                    source,
                    &row.step,
                    row.outcome.as_deref(),
                    row.refusal.as_deref(),
                    Some(&error.message),
                )
                .map_err(|error| error.message)?;
                self.health
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .deferred_no_identity += 1;
                Ok(())
            }
            Err(error) => {
                tracing::warn!(target: "session_conversion", path = %source.path, source = %source.id, class = %error.class, detail = %error.message, "session conversion failed");
                // The --convert-clone command runs without daemon logging, so warn on stderr too.
                if self.stderr_warnings {
                    eprintln!(
                        "WARN session_conversion path={} class={} detail={:?}",
                        source.id, error.class, error.message
                    );
                }
                if error.terminal {
                    self.step_with_detail(
                        source,
                        "awaiting_backup",
                        Some("refused"),
                        Some(&error.class),
                        Some(&error.message),
                    )
                    .map_err(|error| error.message)?;
                    if let Some(address) = source_address(source) {
                        self.health
                            .write()
                            .unwrap_or_else(|p| p.into_inner())
                            .failed_sessions
                            .insert(address, error.class);
                    }
                    if let Err(backup) = self.backup_ready(source) {
                        if backup.class == "crash" {
                            return Err(backup.message);
                        }
                        self.record_error(backup.message);
                    }
                } else {
                    let current = self
                        .ledger
                        .row(&source.id)?
                        .ok_or("conversion source is absent")?;
                    // Once a head or terminal refusal is committed, a backup failure must
                    // not erase that outcome. Recovery still needs its original intent.
                    if current
                        .outcome
                        .as_deref()
                        .is_some_and(|outcome| outcome != "failed")
                        && error.class != "source_changed"
                    {
                        self.record_error(error.message);
                        return Ok(());
                    }
                    self.step_with_detail(
                        source,
                        &current.step,
                        Some("failed"),
                        Some(&error.class),
                        Some(&error.message),
                    )
                    .map_err(|error| error.message)?;
                    if let Some(address) = source_address(source) {
                        self.health
                            .write()
                            .unwrap_or_else(|p| p.into_inner())
                            .failed_sessions
                            .insert(address, error.class);
                    }
                }
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn resume_one_for_test(
        &self,
        id: &str,
        deployment: &DeploymentKey,
        runtime: &broca_module::store_open::StoreRuntime,
        registry: &broca_module::LineageRegistry,
    ) -> Result<(), String> {
        let row = self.ledger.row(id)?.ok_or("conversion source is absent")?;
        self.process(
            &row.source,
            deployment,
            Some(runtime),
            Some(registry),
            false,
            false,
        )
        .await
    }

    async fn convert_one(
        &self,
        row: &Row,
        deployment: &DeploymentKey,
        runtime: Option<&broca_module::store_open::StoreRuntime>,
        registry: Option<&broca_module::LineageRegistry>,
        background: bool,
    ) -> Result<(), Failure> {
        let source = &row.source;
        if matches!(row.step.as_str(), "awaiting_backup" | "backing_up") {
            return self.backup_ready(source);
        }
        let destination = class(source)?.store_root(&self.root);
        let address = source_address(source).ok_or_else(|| io("source has no session address"))?;
        let staging = self.root.join("conversion-staging").join(&address);
        let path = self.root.join(&source.path);
        if row.step != "publishing"
            && matches!(
                broca_wal::store_ownership(&self.root, &address).map_err(io)?,
                broca_wal::StoreOwnership::Held
            )
        {
            self.step(source, "awaiting_backup", Some("already_owned"), None)?;
            return self.backup_ready(source);
        }
        // Live-first lookup must never replace a newer live prefix with a sealed snapshot.
        if source.member.is_some()
            && ["wal", "wal-durable", "wal-ephemeral"]
                .iter()
                .any(|folder| {
                    self.root
                        .join(folder)
                        .join(format!("{address}.wal"))
                        .exists()
                })
        {
            self.step(source, "awaiting_backup", Some("superseded"), None)?;
            return self.backup_ready(source);
        }
        let unidentified = |error: Failure| {
            if background {
                Failure::failed(
                    "deferred_no_identity",
                    format!("{}: {}", error.class, error.message),
                )
            } else {
                error
            }
        };
        let (records, _, _) = read_source(&path, source.member.as_deref()).map_err(unidentified)?;
        let session = records
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RunStarted { session, .. } => Some(session.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                unidentified(Failure::refused("damaged", "source contains no RunStarted"))
            })?;
        let session = broca_session_convert::identity::effective_identity(
            &session,
            &address,
            self.source_index
                .as_ref()
                .map(|index| index.as_ref() as &dyn broca_core::SessionRefResolver),
        )
        .map_err(|error| match error {
            broca_session_convert::identity::IdentityError::Unresolved(detail) => {
                unidentified(Failure::refused("identity_unresolved", detail))
            }
            broca_session_convert::identity::IdentityError::Inventory(detail) => io(detail),
        })?;
        let leases = broca_core::FileLeaseStore::new(self.root.join("leases"));
        let _idle_fence = if background {
            let registry =
                registry.ok_or_else(|| io("background conversion requires a registry"))?;
            Some(
                registry
                    .try_fence_idle_conversion(&session, &leases)
                    .await
                    .map_err(|error| Failure::failed("session_busy", error))?
                    .ok_or_else(|| {
                        Failure::failed("session_busy", "session is not closed and idle")
                    })?,
            )
        } else {
            None
        };
        #[cfg(test)]
        if background {
            if let Some(probe) = &self.background_fenced {
                probe.sessions.lock().unwrap().push(address.clone());
                probe.entered.notify_one();
                probe.release.acquire().await.unwrap().forget();
            }
        }
        let _fence = match registry {
            Some(registry) if !background => Some(
                registry
                    .fence_store_move(&session, &leases)
                    .await
                    .map_err(|error| Failure::failed("session_busy", format!("{error:?}")))?,
            ),
            _ => None,
        };
        let _lease = if registry.is_none() {
            use broca_core::LeaseStore;
            Some(
                leases
                    .acquire(&broca_core::lease_key_for(&session))
                    .map_err(io)?,
            )
        } else {
            None
        };
        // Re-read under the fence: a send may have restored an archive member to a live WAL
        // between discovery and this member's turn.
        if source.member.is_some()
            && ["wal", "wal-durable", "wal-ephemeral"]
                .iter()
                .any(|folder| {
                    self.root
                        .join(folder)
                        .join(format!("{address}.wal"))
                        .exists()
                })
        {
            self.step(source, "awaiting_backup", Some("superseded"), None)?;
            return self.backup_ready(source);
        }
        let (mut records, mut lineage, mut source_sha256) =
            read_source(&path, source.member.as_deref())?;
        if source.member.is_some() {
            // A CLI fold may seal a newer snapshot between background members. The shared
            // archive-pass lease makes this check stable through publication of this member.
            let newest = self.newest_archive(&address)?.ok_or_else(|| {
                Failure::failed(
                    "newer_archive",
                    "the source no longer resolves as the newest archive",
                )
            })?;
            let newest_sha256: String = newest
                .info
                .payload_sha256
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            if newest_sha256 != source_sha256 {
                return Err(Failure::failed(
                    "newer_archive",
                    "a newer sealed archive holds a different session prefix",
                ));
            }
        }
        if matches!(
            row.step.as_str(),
            "staging" | "verifying" | "verified" | "publishing"
        ) && row.source_sha256 != source_sha256
        {
            return Err(Failure::failed(
                "source_changed",
                "source bytes differ from the prefix bound before staging",
            ));
        }
        if let Some(feature) = preconditions::unsupported_in_wal(&records) {
            return Err(Failure::refused(
                feature,
                "legacy feature cannot be carried",
            ));
        }
        if source.member.is_none() && row.step != "publishing" {
            self.step(source, "recovering", None, None)?;
            recover_live(&self.root, &path, &records)?;
            (records, lineage, source_sha256) = read_source(&path, None)?;
        }
        if let Some(reason) = preconditions::wal_refusal(&records, lineage) {
            return Err(Failure::failed(reason, "source still has unfinished work"));
        }
        if let Some(mover) = self.mover.get() {
            mover
                .check_idle(&session, &records, lineage)
                .map_err(|error| {
                    let class = error
                        .detail
                        .as_ref()
                        .and_then(|detail| detail.get("reason"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("io_failed");
                    Failure::failed(
                        if background { "session_busy" } else { class },
                        error.message,
                    )
                })?;
        }
        if let Some(index) = &self.source_index {
            if !preconditions::run_rows_settled(
                &index.session_runs(&session).map_err(io)?,
                preconditions::paused_for_sign_in(&records).as_deref(),
                index.session_exports_settled(&session).map_err(io)?,
            ) {
                return Err(Failure::failed(
                    if background {
                        "session_busy"
                    } else {
                        "reconcile_owed"
                    },
                    "legacy run index still owes reconciliation or usage export",
                ));
            }
        }
        let source_class = class(source)?;
        let media = crate::store_move::media_for_class(&self.root, source_class, &records)
            .map_err(|error| io(error.message))?;
        let media_bytes = media.as_ref().map(|media| &media.store as &dyn MediaBytes);
        let lineage_text = lineage.map(|lineage| lineage.to_string());
        let prepared =
            broca_session_convert::stage::prepare(&records, lineage_text.as_deref(), media_bytes)
                .map_err(stage_failure)?;
        // A session compacted under the first compaction protocol moves read-only: its reads
        // move, and the new run loop refuses a new send to it (`converted_refusal` names it). The
        // verifier rebuilds none of its
        // requests and skips its first open, and the ledger says so.
        let todays_updated_at = broca_module::head_time::updated_at_of_wal(
            &records,
            broca_module::head_time::file_written_ms(&path),
        );
        if row.step == "publishing" {
            if let Some(runtime) = runtime {
                crate::store_move::StoreMove::hold_parts(
                    runtime,
                    source_class,
                    &prepared.converted,
                )
                .map_err(|error| io(error.message))?;
            }
        }
        if row.step != "publishing" {
            self.ledger
                .bind_source(&source.id, &source_sha256)
                .map_err(io)?;
            self.step(source, "staging", None, None)?;
            remove_staging(&staging)?;
            self.directories
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .ensure_parent(&self.root, staging.parent().expect("staging has a parent"))
                .map_err(io)?;
            let history_version = source_history(
                self.source_index.as_deref(),
                &session,
                lineage_text.clone(),
                registry,
            )
            .await?;
            let moved = broca_session_store::record::Moved {
                source_lineage: None,
                move_id: format!("cutover:{address}"),
                source_activity_ms: todays_updated_at,
            };
            #[cfg(test)]
            let interrupted = std::sync::atomic::AtomicBool::new(false);
            #[cfg(test)]
            let callback = |step: broca_session_store::backend::unpublished::Step| {
                if self.crash == Some(step.name()) || self.faults.crash_here(step.name()) {
                    interrupted.store(true, std::sync::atomic::Ordering::Relaxed);
                    return Err(std::io::Error::other("conversion interrupted"));
                }
                Ok(())
            };
            #[cfg(test)]
            let progress: broca_session_store::backend::unpublished::Progress<'_> = Some(&callback);
            #[cfg(not(test))]
            let progress = None;
            let staged = broca_session_convert::stage::write_with_progress(
                &Stage {
                    records: &records,
                    lineage: lineage_text.as_deref(),
                    source_history_version: history_version,
                    session: &session,
                    deployment,
                    store_root: &destination,
                    staging: &staging,
                    media: media_bytes,
                    moved: &moved,
                },
                prepared,
                progress,
            )
            .await;
            #[cfg(test)]
            if interrupted.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Failure::failed("crash", "staging interrupted"));
            }
            let staged = staged.map_err(stage_failure)?;
            self.step(source, "verifying", None, None)?;
            #[cfg(test)]
            let verification_records = {
                let mut compared = records.clone();
                if let Some(change) = self.verification_records {
                    change(&mut compared);
                }
                compared
            };
            #[cfg(test)]
            let records_to_verify = &verification_records;
            #[cfg(not(test))]
            let records_to_verify = &records;
            let verified = broca_session_convert::verify::verify_with_progress(
                Verify {
                    records: records_to_verify,
                    lineage,
                    session: &session,
                    deployment,
                    store_root: &destination,
                    staging: &staging,
                    converted: &staged.converted,
                    mode: Mode::Move,
                    media: media_bytes,
                    media_root: media.as_ref().map(|media| MediaRoot {
                        state_root: &self.root,
                        class: media.class,
                    }),
                    todays_updated_at,
                },
                progress,
            )
            .await;
            #[cfg(test)]
            if interrupted.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Failure::failed("crash", "verification interrupted"));
            }
            let verified =
                verified.map_err(|error| Failure::failed("verification_failed", error))?;
            // Keep historical replay limits and provider-rejected requests visible after
            // publication; neither category skips read, history or fresh-send verification.
            let request_limits = verified.request_limits_detail();
            if let Some(detail) = &request_limits {
                tracing::warn!(target: "session_conversion", source = %source.id, "{detail}");
            }
            let read_only = verified.read_only.then_some(READ_ONLY_DETAIL);
            let detail = match (request_limits, read_only) {
                (Some(limit), Some(read_only)) => Some(format!("{read_only}; {limit}")),
                (limit, read_only) => limit.or(read_only.map(str::to_owned)),
            };
            if let Some(runtime) = runtime {
                crate::store_move::StoreMove::hold_parts(runtime, source_class, &staged.converted)
                    .map_err(|error| io(error.message))?;
            }
            self.step_with_detail(source, "verified", None, None, detail.as_deref())?;
            if let Some(inventory) = &self.inventory {
                use broca_core::SessionRefIndex;
                inventory.record_session(&address, &session).map_err(io)?;
            }
            broca_wal::raise_floor(
                &self.root.join(broca_wal::FEATURE_FLOOR_FILENAME),
                &[broca_wal::SESSION_STORAGE_FEATURE],
            )
            .map_err(io)?;
            self.step(source, "publishing", None, None)?;
        }
        crate::store_move::publish_data_at(
            &self.root,
            &staging,
            &destination,
            &address,
            &mut self.directories.lock().unwrap_or_else(|p| p.into_inner()),
            |_boundary| {
                #[cfg(test)]
                if self.crash == Some(_boundary) {
                    return Err(crate::store_move::Stop::Crashed);
                }
                Ok(())
            },
        )
        .map_err(move_failure)?;
        crate::store_move::publish_head_at(&staging, &destination, &address, None)
            .await
            .map_err(move_failure)?;
        #[cfg(test)]
        if self.crash == Some("publish_head") {
            return Err(Failure::failed("crash", "publish_head"));
        }
        if let Some(registry) = registry {
            registry.forget_moved_session(&session);
        }
        if let Some(mover) = self.mover.get() {
            mover
                .retire_converted(&session)
                .map_err(|error| io(error.message))?;
        }
        // The published head now owns all requests; source backup cannot affect its writer.
        drop(_idle_fence);
        // The steps since verification carried its note (a replay limit) forward, also across a
        // restart that resumed at publication; it stays on the converted row.
        let note = self
            .ledger
            .row(&source.id)
            .map_err(io)?
            .and_then(|row| row.detail);
        self.step_with_detail(
            source,
            "awaiting_backup",
            Some("converted"),
            None,
            note.as_deref(),
        )?;
        self.backup_ready(source)?;
        self.health
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .failed_sessions
            .remove(&address);
        remove_staging(&staging)?;
        Ok(())
    }

    fn backup_ready(&self, source: &Source) -> Result<(), Failure> {
        if source.member.is_some() && self.ledger.unfinished_path(&source.path).map_err(io)? {
            return Ok(());
        }
        let mut rows = if source.member.is_some() {
            self.ledger.rows_for_path(&source.path).map_err(io)?
        } else {
            vec![self
                .ledger
                .row(&source.id)
                .map_err(io)?
                .ok_or_else(|| io("source row missing"))?]
        };
        if rows
            .iter()
            .any(|row| !matches!(row.step.as_str(), "awaiting_backup" | "backing_up" | "done"))
        {
            return Ok(());
        }
        let original = self.root.join(&source.path);
        let backup = PathBuf::from(&source.backup);
        let bound_source = if original.try_exists().map_err(io)? {
            &original
        } else {
            &backup
        };
        for row in &mut rows {
            if row.source_sha256.is_empty() {
                continue;
            }
            let matches = source_digest(bound_source, row.source.member.as_deref())
                .map(|digest| digest == row.source_sha256)
                .or_else(|error| {
                    if error.terminal {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                })?;
            if !matches {
                self.step(
                    &row.source,
                    &row.step,
                    Some("failed"),
                    Some("source_changed"),
                )?;
                if let Some(address) = source_address(&row.source) {
                    self.health
                        .write()
                        .unwrap_or_else(|p| p.into_inner())
                        .failed_sessions
                        .insert(address, "source_changed".into());
                }
                return Err(Failure::failed(
                    "source_changed",
                    "source bytes differ from the prefix bound before staging",
                ));
            }
            if row.outcome.as_deref() == Some("failed")
                && row.refusal.as_deref() == Some("source_changed")
            {
                row.outcome = Some("converted".into());
                row.refusal = None;
            }
        }
        for row in &rows {
            if row.step != "done" {
                self.step(
                    &row.source,
                    "backing_up",
                    row.outcome.as_deref(),
                    row.refusal.as_deref(),
                )?;
            }
        }
        let parent = backup.parent().ok_or_else(|| io("backup has no parent"))?;
        broca_owner_only::create_dir_all(parent).map_err(io)?;
        broca_wal::migration::durable_dir(&self.backup, parent).map_err(io)?;
        if original.try_exists().map_err(io)? {
            if backup.try_exists().map_err(io)? {
                return Err(io(
                    "source and converted backup both exist; refusing to overwrite either",
                ));
            }

            std::fs::rename(&original, &backup).map_err(io)?;
        } else if !backup.try_exists().map_err(io)?
            && rows
                .iter()
                .any(|row| row.refusal.as_deref() != Some("damaged"))
        {
            return Err(io("conversion source and backup are both missing"));
        }
        // Re-establish both rename barriers even when recovery observes the backup already
        // present: readable directory entries alone do not prove the rename was durable.
        broca_wal::migration::sync_dir(backup.parent().ok_or_else(|| io("backup has no parent"))?)
            .map_err(io)?;
        if let Some(parent) = original.parent().filter(|parent| parent.exists()) {
            broca_wal::migration::sync_dir(parent).map_err(io)?;
        }
        for row in rows {
            if let Some(address) = source_address(&row.source) {
                remove_staging(&self.root.join("conversion-staging").join(address))?;
            }
            self.step(
                &row.source,
                "done",
                row.outcome.as_deref(),
                row.refusal.as_deref(),
            )?;
            if matches!(row.outcome.as_deref(), Some("converted" | "already_owned")) {
                if let Some(address) = source_address(&row.source) {
                    self.health
                        .write()
                        .unwrap_or_else(|p| p.into_inner())
                        .failed_sessions
                        .remove(&address);
                }
            }
        }
        Ok(())
    }
}

fn move_failure(error: crate::store_move::Stop) -> Failure {
    match error {
        crate::store_move::Stop::Refused(error) => io(error.message),
        #[cfg(test)]
        crate::store_move::Stop::Crashed => Failure::failed("crash", "publication crash"),
    }
}

fn stage_failure(error: StageError) -> Failure {
    match error {
        StageError::NotMovable(reason) => Failure::failed(reason, reason),
        StageError::MediaMissing(missing) => {
            Failure::failed("media_missing", format!("{missing:?}"))
        }
        StageError::Lossy(reason) => Failure::failed("conversion_lossy", reason),
        StageError::Failed(reason) => io(reason),
    }
}

fn remove_staging(path: &Path) -> Result<(), Failure> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io(error)),
    }
}

fn read_source(
    path: &Path,
    member: Option<&str>,
) -> Result<(Vec<FramedRecord>, Option<WalLineageId>, String), Failure> {
    let (read, length, digest) = if let Some(member) = member {
        let member = archived_source(path, member)?;
        let length = member.payload.len() as u64;
        (
            broca_wal::file_wal::read_verified_member_range(&member.payload, 0, 0, None)
                .map_err(source_wal)?,
            length,
            member
                .info
                .payload_sha256
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
    } else {
        let length = std::fs::metadata(path).map_err(source_io)?.len();
        (
            broca_wal::read_records_range(path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
                .map_err(source_wal)?,
            length,
            source_digest(path, None)?,
        )
    };
    if length == 0 || read.complete_byte_end != Some(length) {
        return Err(Failure::refused(
            "damaged",
            "log is empty or does not decode completely",
        ));
    }
    let lineage = read.lineage_id;
    match read.into_records() {
        broca_wal::WalRangeRecords::Complete(records) if !records.is_empty() => {
            Ok((records, lineage, digest))
        }
        _ => Err(Failure::refused(
            "damaged",
            "log contains no complete records",
        )),
    }
}

fn source_digest(path: &Path, member: Option<&str>) -> Result<String, Failure> {
    use sha2::{Digest, Sha256};
    if let Some(member) = member {
        let member = archived_source(path, member)?;
        return Ok(member
            .info
            .payload_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect());
    }
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(source_io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(source_io)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn archived_source(
    path: &Path,
    member: &str,
) -> Result<broca_wal::archive::ArchiveMember, Failure> {
    // The archive resolver shares its verified membership cache across reads; reopening and
    // decoding the whole index for every member would make large containers quadratic.
    broca_wal::archive::read_newest_member(
        &[path.to_path_buf()],
        member,
        broca_wal::DEFAULT_MAX_READ_BYTES,
    )
    .map_err(source_io)?
    .ok_or_else(|| Failure::refused("damaged", "archive member is missing"))
}

fn source_io(error: std::io::Error) -> Failure {
    match error.kind() {
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::InvalidData
        | std::io::ErrorKind::UnexpectedEof => Failure::refused("damaged", error),
        _ => io(error),
    }
}

fn source_wal(error: broca_wal::WalError) -> Failure {
    match error {
        broca_wal::WalError::Io(error) => source_io(error),
        error @ broca_wal::WalError::NewerWriter { .. } => Failure::failed("newer_writer", error),
        error @ broca_wal::WalError::TooLarge { .. } => Failure::failed("source_too_large", error),
        error => Failure::refused("damaged", error),
    }
}

/// Use the actor's seal planner, not a conversion-specific interpretation of dispatched calls.
/// A restart never dispatches tools: uncertain calls become outcome_unknown before Interrupted.
#[cfg(test)]
#[path = "session_conversion/recover_tests.rs"]
mod recover_tests;

/// The ledger detail of a session converted read-only.
const READ_ONLY_DETAIL: &str =
    "read_only: compacted under the first compaction protocol, so a new \
send is refused; its first open, next request and recorded requests were not checked";

fn recover_live(root: &Path, path: &Path, records: &[FramedRecord]) -> Result<(), Failure> {
    let raw = records
        .iter()
        .map(|frame| frame.record.clone())
        .collect::<Vec<_>>();
    let prior = broca_core::replay(&raw).map_err(io)?;
    let Some(run) = prior.run_id.as_ref() else {
        return Ok(());
    };
    // Only a run a crash cut is sealed. A paused run stays paused, as today's open leaves it:
    // its resume, cancel or interrupt is its caller's, and the new storage serves it paused.
    if broca_core::reconcile_run_state(&raw, &run.0).is_some_and(|state| {
        state.state.is_terminal() || state.state == broca_core::RunIndexState::Paused
    }) {
        return Ok(());
    }
    let seal = broca_core::resume::interrupted_seal(
        &prior,
        broca_core::resume::SealCause::CrashOrphan,
        None,
        false,
        records.last().map(|frame| frame.seq),
        Some(broca_core::resume::crash_orphan_error()),
        |_| broca_core::DeferredCloseKind::NotDispatched,
    );
    let mut wal = broca_wal::FileWal::open(path.to_path_buf())
        .map_err(io)?
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    if let Some(record) = seal.resume_started {
        wal.append(0, &record, false).map_err(io)?;
    }
    for record in &seal.close_out {
        wal.append(0, record, false).map_err(io)?;
    }
    if seal.close_out_syncs_first {
        wal.sync().map_err(io)?;
    }
    wal.append(
        0,
        &seal.terminal.record(broca_core::run::epoch_millis()),
        true,
    )
    .map_err(io)?;
    Ok(())
}

async fn source_history(
    index: Option<&broca_store::SqliteStore>,
    session: &BindIdentity,
    lineage: Option<String>,
    registry: Option<&broca_module::LineageRegistry>,
) -> Result<u64, Failure> {
    if let Some(registry) = registry {
        let observed = registry.history_read_version(session).await.map_err(io)?;
        return registry
            .served_snapshot(session, lineage, true, observed)
            .await
            .map(|snapshot| snapshot.history_version)
            .map_err(io);
    }
    use broca_core::ServedHistory;
    match index {
        None => Ok(1),
        Some(index) => index
            .history_read_version(&broca_core::session_addr(session))
            .map(|version| version.unwrap_or(1))
            .map_err(io),
    }
}

#[derive(Serialize)]
pub struct Rehearsal {
    pub progress: Progress,
    pub live_ms: u128,
    pub total_ms: u128,
    pub reconcile_owed_sessions: Vec<String>,
}

/// Run only on an operator-supplied clone. No daemon, vault, provider, or export write is used.
pub async fn rehearse(root: &Path, backup: &Path) -> Result<Rehearsal, String> {
    let started = Instant::now();
    // A clone carries the lock file's bytes, not the running daemon's OS lock. Refuse a
    // genuinely running root before opening the ledger or inspecting any source.
    let _root_lock = crate::state_root_lock::acquire(root).map_err(|error| error.to_string())?;
    let mut engine = Engine::configured(
        root,
        Config {
            archives: true,
            live_at_startup: true,
            backup_root: Some(backup.to_path_buf()),
        },
    )?
    .ok_or("conversion is disabled")?;
    engine.stderr_warnings = true;
    engine.parallelism = parallel::clone_workers();
    let deployment = DeploymentKey::unloaded(1);
    engine.run_live(&deployment, None, None).await?;
    let live_ms = started.elapsed().as_millis();
    engine
        .run_archives(&deployment, None, None, Duration::ZERO)
        .await?;
    let reconcile_owed_sessions = engine
        .ledger
        .rows()?
        .iter()
        .filter(|row| row.refusal.as_deref() == Some("reconcile_owed"))
        .filter_map(|row| source_address(&row.source))
        .collect();
    Ok(Rehearsal {
        progress: engine.ledger.progress()?,
        live_ms,
        total_ms: started.elapsed().as_millis(),
        reconcile_owed_sessions,
    })
}

/// The daemon's recovery task owns sealing and export backfill. Its completion is a hard
/// prerequisite, not a job launched concurrently with conversion of the same live sources.
pub async fn after_startup_recovery(
    recovery: impl std::future::Future<Output = Result<(), String>>,
    conversion: impl std::future::Future<Output = Result<(), String>>,
) -> Result<(), String> {
    recovery.await?;
    conversion.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cutover_rehearsal_refuses_a_running_root_before_discovery() {
        let root = broca_test_support::scratch_root().join("conversion-running-root");
        std::fs::create_dir_all(&root).unwrap();
        let _lock = crate::state_root_lock::acquire(&root).unwrap();
        let result = rehearse(&root, &root.with_extension("sources")).await;
        assert!(result.is_err());
        assert!(!root.join(crate::conversion_ledger::FILE).exists());
    }

    #[test]
    fn cutover_missing_empty_and_garbage_are_damage_but_io_denials_are_not() {
        let root = broca_test_support::scratch_root().join("conversion-damage");
        std::fs::create_dir_all(&root).unwrap();
        for (name, bytes) in [
            ("empty", Some(&b""[..])),
            ("garbage", Some(&b"garbage"[..])),
            ("missing", None),
        ] {
            let path = root.join(format!("{name}.wal"));
            if let Some(bytes) = bytes {
                std::fs::write(&path, bytes).unwrap();
            }
            let failure = read_source(&path, None).unwrap_err();
            assert_eq!(failure.class, "damaged", "{name}");
            assert!(failure.terminal, "{name}");
        }
        assert!(!source_io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).terminal);
        let newer = source_wal(broca_wal::WalError::NewerWriter {
            offset: 0,
            ver: 5,
            missing: vec!["future-feature".into()],
        });
        assert_eq!(newer.class, "newer_writer");
        assert!(!newer.terminal);
    }

    #[test]
    fn default_conversion_job_opens_no_source() {
        let root = broca_test_support::scratch_root().join("disabled-conversion-must-not-exist");
        assert!(!root.exists());
        assert_eq!(Config::parse("{}").unwrap(), Config::default());
        assert!(Engine::configured(&root, Config::default())
            .unwrap()
            .is_none());
        assert!(!root.exists());
        let config = Config::parse(r#"{"session_conversion":{"archives":true}}"#).unwrap();
        assert!(config.archives);
        assert!(!config.live_at_startup);
        assert!(Config::parse(r#"{"session_conversion":{"enabled":true}}"#).is_err());
    }
}
