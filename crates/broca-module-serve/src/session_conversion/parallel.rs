//! Bounded CPU workers; source conflicts are resolved before entering per-session conversion.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use broca_session_store::keys::DeploymentKey;
use tokio_util::sync::CancellationToken;

use super::{source_address, Engine, Source};

pub(super) struct Pass<'a> {
    pub deployment: &'a DeploymentKey,
    pub runtime: Option<&'a Arc<broca_module::store_open::StoreRuntime>>,
    pub registry: Option<&'a broca_module::LineageRegistry>,
    pub background: bool,
    pub throttle: Duration,
    pub lease: Arc<tokio::sync::OwnedMutexGuard<()>>,
}

// Serving keeps conversion to two cores. A clone can use half the machine, capped at eight,
// leaving headroom for the operator and avoiding unbounded concurrent disk syncs.
pub(super) const DAEMON_WORKERS: usize = 2;
const CLONE_MAX_WORKERS: usize = 8;

pub(super) fn clone_workers() -> usize {
    workers_for_parallelism(std::thread::available_parallelism().map_or(1, usize::from))
}

fn workers_for_parallelism(available: usize) -> usize {
    (available / 2).clamp(1, CLONE_MAX_WORKERS)
}

struct Work {
    source: Source,
    group: usize,
}

#[derive(Default)]
struct Queue {
    groups: BTreeMap<usize, VecDeque<(usize, Source)>>,
    ready: BTreeMap<usize, usize>,
    stopped: bool,
    error: Option<String>,
}

impl Queue {
    fn set_work(&mut self, work: VecDeque<Work>) {
        for (position, work) in work.into_iter().enumerate() {
            let group = self.groups.entry(work.group).or_default();
            if group.is_empty() {
                self.ready.insert(position, work.group);
            }
            group.push_back((position, work.source));
        }
    }
}

#[derive(Default)]
struct Scheduler {
    queue: Mutex<Queue>,
    changed: Condvar,
    cancel: CancellationToken,
}

impl Scheduler {
    fn next(&self, stopping: &CancellationToken) -> Option<Work> {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if queue.stopped || stopping.is_cancelled() || queue.groups.is_empty() {
                return None;
            }
            if let Some((_, group)) = queue.ready.pop_first() {
                let (_, source) = queue
                    .groups
                    .get_mut(&group)
                    .expect("ready group exists")
                    .pop_front()
                    .expect("ready group has a source");
                return Some(Work { source, group });
            }
            queue = self.changed.wait(queue).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn finish(&self, group: usize) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((position, _)) = queue.groups[&group].front() {
            let position = *position;
            queue.ready.insert(position, group);
        } else {
            queue.groups.remove(&group);
        }
        self.changed.notify_all();
    }

    fn stop(&self, error: Option<String>) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.stopped = true;
        if let Some(error) = error {
            queue.error.get_or_insert(error);
            self.cancel.cancel();
        }
        self.changed.notify_all();
    }
}

struct CancelOnDrop(Arc<Scheduler>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.stop(None);
        self.0.cancel.cancel();
    }
}

// Moving an archive container to backup also removes its members' staging directories,
// including directories named by completed rows. Include every ledger row when connecting
// paths, backups and addresses. Transitive groups keep overlapping archive snapshots newest-first.
fn pending_work(engine: &Engine, sources: Vec<Source>) -> Result<VecDeque<Work>, String> {
    let rows = engine.ledger.rows()?;
    let mut parents: Vec<_> = (0..rows.len()).collect();
    let mut resources = BTreeMap::new();
    fn root(parents: &mut [usize], mut index: usize) -> usize {
        while parents[index] != index {
            parents[index] = parents[parents[index]];
            index = parents[index];
        }
        index
    }
    for (index, row) in rows.iter().enumerate() {
        let source = &row.source;
        let mut keys = vec![(0, source.path.clone()), (1, source.backup.clone())];
        if let Some(address) = source_address(source) {
            keys.push((2, address));
        }
        for key in keys {
            if let Some(other) = resources.insert(key, index) {
                let a = root(&mut parents, index);
                let b = root(&mut parents, other);
                parents[a] = b;
            }
        }
    }
    let by_id: BTreeMap<_, _> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (&row.source.id, (i, row)))
        .collect();
    let mut seen = BTreeSet::new();
    let mut pending = VecDeque::new();
    for source in sources {
        if !seen.insert(source.id.clone()) {
            continue;
        }
        let (index, row) = by_id.get(&source.id).ok_or("conversion source is absent")?;
        if row.step != "done" {
            pending.push_back(Work {
                source: row.source.clone(),
                group: root(&mut parents, *index),
            });
        }
    }
    Ok(pending)
}

impl Engine {
    pub(super) async fn run_pass(
        &self,
        sources: Vec<Source>,
        pass: Pass<'_>,
    ) -> Result<(), String> {
        let scheduler = Arc::new(Scheduler::default());
        let pending = pending_work(self, sources)?;
        let workers = self.parallelism.min(pending.len());
        let stopping = pass
            .registry
            .map(|r| r.stopping_signal())
            .unwrap_or_default();
        self.health
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .live_remaining = self
            .ledger
            .rows()?
            .iter()
            .filter(|row| row.source.member.is_none() && row.step != "done")
            .count() as u64;
        if workers == 0 || stopping.is_cancelled() {
            return Ok(());
        }
        // One shared lease excludes external archive creation and restoration for the entire
        // pass. Taking the exclusive OS lease in each worker would make them refuse one another.
        let archive_lease = if pending.iter().any(|work| work.source.member.is_some()) {
            Some(Arc::new(Mutex::new(
                broca_core::lease::acquire_archive_pass(&self.root)
                    .map_err(|error| error.to_string())?,
            )))
        } else {
            None
        };
        scheduler
            .queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_work(pending);
        let _cancel_on_drop = CancelOnDrop(scheduler.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..workers {
            let engine = self.clone();
            let deployment = pass.deployment.duplicate_for_worker();
            let runtime = pass.runtime.cloned();
            let registry = pass.registry.cloned();
            let background = pass.background;
            let throttle = pass.throttle;
            let pass_lease = pass.lease.clone();
            let scheduler = scheduler.clone();
            let stopping = stopping.clone();
            let archive_lease = archive_lease.clone();
            let handle = tokio::runtime::Handle::current();
            // Conversion has long synchronous CPU and fsync stretches. Running only joined
            // futures on the serving executor would interleave them, not parallelize them.
            tasks.spawn_blocking(move || {
                let _archive_lease = archive_lease;
                // Keep admission serialized even if the caller drops its future before the
                // blocking workers have reached an await and released their session fences.
                let _pass_lease = pass_lease;
                while let Some(work) = scheduler.next(&stopping) {
                    let result = handle.block_on(async {
                        tokio::select! {
                            biased;
                            _ = scheduler.cancel.cancelled() => Ok(()),
                            result = async {
                                #[cfg(test)]
                                let _probe = if let Some(probe) = &engine.parallel_probe {
                                    Some(probe.enter(&work.source).await)
                                } else { None };
                                engine.process(&work.source, &deployment, runtime.as_deref(),
                                    registry.as_ref(), background && work.source.member.is_none(), true).await?;
                                if work.source.member.is_none() {
                                    let row = engine.ledger.row(&work.source.id)?.ok_or("conversion source is absent")?;
                                    if row.step == "done" {
                                        let mut health = engine.health.write().unwrap_or_else(|p| p.into_inner());
                                        health.live_remaining = health.live_remaining.saturating_sub(1);
                                        if background && row.outcome.as_deref() == Some("converted") {
                                            health.live_background_converted += 1;
                                        }
                                    }
                                }
                                Ok(())
                            } => result,
                        }
                    });
                    if let Err(error) = result {
                        scheduler.stop(Some(error));
                    }
                    scheduler.finish(work.group);
                    if !throttle.is_zero() {
                        handle.block_on(async {
                            tokio::select! {
                                _ = scheduler.cancel.cancelled() => {},
                                _ = stopping.cancelled() => {},
                                _ = tokio::time::sleep(throttle) => {},
                            }
                        });
                    }
                }
            });
        }
        let mut draining = false;
        let mut panic = None;
        while !tasks.is_empty() {
            tokio::select! {
                biased;
                _ = stopping.cancelled(), if !draining => {
                    scheduler.stop(None);
                    draining = true;
                }
                result = tasks.join_next() => {
                    if let Some(Err(error)) = result {
                        scheduler.stop(Some(format!("conversion worker failed: {error}")));
                        if error.is_panic() && panic.is_none() {
                            panic = Some(error.into_panic());
                        }
                    }
                }
            }
        }
        if let Some(panic) = panic {
            std::panic::resume_unwind(panic);
        }
        let error = scheduler
            .queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .error
            .take();
        error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
pub(crate) struct Probe {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Semaphore,
    pub sources: Mutex<Vec<Source>>,
    pub crashes: BTreeMap<String, &'static str>,
    pub crash_barrier: Option<std::sync::Barrier>,
    pub active: Mutex<BTreeMap<String, usize>>,
    pub overlap: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl Probe {
    async fn enter(&self, source: &Source) -> ProbeGuard<'_> {
        let guard = ProbeGuard {
            probe: self,
            path: source.path.clone(),
        };
        {
            let mut active = self.active.lock().unwrap();
            let count = active.entry(source.path.clone()).or_default();
            if *count > 0 {
                self.overlap
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            *count += 1;
        }
        self.sources.lock().unwrap().push(source.clone());
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        guard
    }

    pub(super) fn crash(&self, source: &Source, step: &str) -> bool {
        if self.crashes.get(&source.id).copied() == Some(step) {
            if let Some(barrier) = &self.crash_barrier {
                barrier.wait();
            }
            return true;
        }
        false
    }
}

#[cfg(test)]
struct ProbeGuard<'a> {
    probe: &'a Probe,
    path: String,
}

#[cfg(test)]
impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        *self
            .probe
            .active
            .lock()
            .unwrap()
            .get_mut(&self.path)
            .unwrap() -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_conversion::Config;
    use broca_wal::StorageClass;

    #[test]
    fn concurrent_conversion_limits_leave_cpu_headroom() {
        for (available, workers) in [(1, 1), (2, 1), (3, 1), (4, 2), (16, 8), (64, 8)] {
            assert_eq!(workers_for_parallelism(available), workers);
        }
        assert_eq!(DAEMON_WORKERS, 2);
    }

    #[test]
    fn concurrent_conversion_conflicts_include_completed_members_and_transitive_addresses() {
        let root = broca_test_support::scratch_root().join("conversion-components");
        std::fs::create_dir_all(&root).unwrap();
        let engine = Engine::configured(
            &root,
            Config {
                archives: true,
                live_at_startup: true,
                backup_root: Some(root.with_extension("backup")),
            },
        )
        .unwrap()
        .unwrap();
        let source = |path: &str, member: Option<&str>| {
            engine
                .source(
                    &root.join(path),
                    member.map(str::to_owned),
                    StorageClass::Ordinary,
                )
                .unwrap()
        };
        let completed = source("wal-archive/fold-2000.ark", Some("a"));
        let first = source("wal-archive/fold-2000.ark", Some("b"));
        let second = source("wal-archive/fold-3000.ark", Some("a"));
        let third = source("wal/a.wal", None);
        let independent = source("wal/c.wal", None);
        let mut shared_backup = source("wal/d.wal", None);
        shared_backup.backup = first.backup.clone();
        for source in [
            &completed,
            &first,
            &second,
            &third,
            &independent,
            &shared_backup,
        ] {
            engine.discover(source).unwrap();
        }
        engine
            .ledger
            .advance(&completed.id, "done", Some("converted"), None, None)
            .unwrap();
        let pending = pending_work(
            &engine,
            vec![
                completed,
                first.clone(),
                second,
                third,
                independent.clone(),
                shared_backup,
                first.clone(),
            ],
        )
        .unwrap();
        assert_eq!(
            pending.len(),
            5,
            "completed and duplicate sources are not admitted"
        );
        let groups: Vec<_> = pending.iter().map(|work| work.group).collect();
        assert_eq!(groups[0], groups[1]);
        assert_eq!(groups[1], groups[2]);
        assert_eq!(groups[0], groups[4]);
        assert_ne!(groups[0], groups[3]);
        let scheduler = Scheduler::default();
        scheduler.queue.lock().unwrap().set_work(pending);
        let stopping = CancellationToken::new();
        let active = scheduler.next(&stopping).unwrap();
        assert_eq!(active.source, first);
        assert_eq!(scheduler.next(&stopping).unwrap().source, independent);
        scheduler.finish(active.group);
        assert_eq!(scheduler.next(&stopping).unwrap().group, active.group);
    }
}
