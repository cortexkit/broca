//! Preserve the daemon's stamped folder and project id. Address resolution uses workdir for
//! new scoped sessions; an existing session keeps its inventory root. The counters compare
//! the stamped folder with that resolved storage root, exposing retained legacy differences.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use broca_protocol::BindIdentity;

/// The folder and project id the daemon stamped on a scoped bind, as broca keeps them.
///
/// The daemon sends both only to modules declaring
/// [`subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY`], and only when the scope's owner
/// registered them. Both are attribution and grant nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StampedProject {
    /// The daemon's canonical session folder, fixed for the scope's epoch.
    pub(crate) workdir: Option<PathBuf>,
    /// The scope owner's opaque project id, fixed for the scope's epoch.
    pub(crate) project_id: Option<String>,
}

impl StampedProject {
    pub(crate) fn from_stamp(stamp: &subc_protocol::scope::ScopeStamp) -> Self {
        Self {
            workdir: stamp.workdir.as_ref().map(PathBuf::from),
            project_id: stamp.project_id.clone(),
        }
    }

    /// Where this session's project is: the stamped folder and project id when the stamp
    /// carries them, otherwise the bind's `project_root` and the bind's own project id.
    pub(crate) fn resolve<'a>(
        stamp: Option<&'a Self>,
        project_root: &'a Path,
        bind_project_id: Option<&'a str>,
    ) -> ProjectLocation<'a> {
        let stamped_workdir = stamp.and_then(|stamp| stamp.workdir.as_deref());
        ProjectLocation {
            workdir: stamped_workdir.unwrap_or(project_root),
            workdir_stamped: stamped_workdir.is_some(),
            project_id: stamp
                .and_then(|stamp| stamp.project_id.as_deref())
                .or(bind_project_id),
        }
    }
}

/// A session's project location, after falling back to the bind for anything the stamp lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectLocation<'a> {
    pub(crate) workdir: &'a Path,
    /// Whether `workdir` came from the stamp rather than the bind's `project_root`.
    pub(crate) workdir_stamped: bool,
    pub(crate) project_id: Option<&'a str>,
}

impl ProjectLocation<'_> {
    /// How this location's folder compares with the folder the session is keyed under. The
    /// comparison is by path components and touches no filesystem: the daemon has already
    /// canonicalized the stamped folder, and the bind root is used exactly as it keys.
    pub(crate) fn compare(&self, project_root: &Path) -> WorkdirComparison {
        if !self.workdir_stamped {
            WorkdirComparison::Absent
        } else if self.workdir == project_root {
            WorkdirComparison::Same
        } else {
            WorkdirComparison::Differs
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkdirComparison {
    /// The stamp names the same folder as the bind's `project_root`.
    Same,
    /// The stamp names a different folder: keying by the stamp would move this session.
    Differs,
    /// The stamp names no folder, so the bind's `project_root` stands in for it.
    Absent,
}

/// How many distinct sessions this tally keeps for de-duplication. Past it, a newly seen
/// session is counted under `untracked` instead, so the other counters never count one
/// session twice and memory stays bounded however many sessions a process serves.
const TALLY_SESSION_LIMIT: usize = 1 << 18;

/// Distinct scoped sessions per comparison outcome, counted once per session per process.
#[derive(Default)]
pub(crate) struct WorkdirTally {
    /// `(session address, outcome)` pairs already counted. A session rebound under a scope
    /// with another folder can appear under more than one outcome, once each.
    seen: Mutex<HashSet<(String, WorkdirComparison)>>,
    same: AtomicU64,
    differs: AtomicU64,
    absent: AtomicU64,
    untracked: AtomicU64,
}

impl WorkdirTally {
    /// Count `session` under `comparison` unless it was already counted there. Returns true
    /// only the first time a session is counted as `Differs`, which is when the caller logs.
    pub(crate) fn record(&self, session_address: &str, comparison: WorkdirComparison) -> bool {
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (session_address.to_owned(), comparison);
        if seen.contains(&key) {
            return false;
        }
        if seen.len() >= TALLY_SESSION_LIMIT {
            self.untracked.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        seen.insert(key);
        drop(seen);
        let counter = match comparison {
            WorkdirComparison::Same => &self.same,
            WorkdirComparison::Differs => &self.differs,
            WorkdirComparison::Absent => &self.absent,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        comparison == WorkdirComparison::Differs
    }

    /// The counters as `health.check` reports them, zeros included.
    pub(crate) fn health(&self) -> serde_json::Value {
        serde_json::json!({
            "sessions_same": self.same.load(Ordering::Relaxed),
            "sessions_differ": self.differs.load(Ordering::Relaxed),
            "sessions_without_workdir": self.absent.load(Ordering::Relaxed),
            "sessions_untracked": self.untracked.load(Ordering::Relaxed),
        })
    }
}

static PROCESS_TALLY: std::sync::OnceLock<WorkdirTally> = std::sync::OnceLock::new();

fn process_tally() -> &'static WorkdirTally {
    PROCESS_TALLY.get_or_init(WorkdirTally::default)
}

/// The process-wide counters for `health.check`.
pub(crate) fn health() -> serde_json::Value {
    process_tally().health()
}

/// Measure one scoped send: compare its stamped folder with the resolved storage root, count
/// the session once per outcome, and log once per session whose folder differs. The log
/// names the session by its hashed address and ref, never by either folder.
pub(crate) fn observe_scoped_send(
    session: &BindIdentity,
    stamp: Option<&StampedProject>,
    bind_project_id: Option<&str>,
) {
    observe_into(process_tally(), session, stamp, bind_project_id);
}

fn observe_into(
    tally: &WorkdirTally,
    session: &BindIdentity,
    stamp: Option<&StampedProject>,
    bind_project_id: Option<&str>,
) -> bool {
    let location = StampedProject::resolve(stamp, &session.project_root, bind_project_id);
    let comparison = location.compare(&session.project_root);
    let address = broca_core::session_addr(session);
    let first_difference = tally.record(&address, comparison);
    if first_difference {
        tracing::info!(
            target: "scope",
            session = %address,
            session_ref = %broca_core::agent_session_ref(session),
            project_id = location.project_id.unwrap_or(""),
            "scoped session's stamped workdir differs from its resolved storage root; the session retains its existing address"
        );
    }
    first_difference
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamped(workdir: Option<&str>, project_id: Option<&str>) -> StampedProject {
        StampedProject {
            workdir: workdir.map(PathBuf::from),
            project_id: project_id.map(str::to_owned),
        }
    }

    #[test]
    fn a_stamp_without_a_folder_or_project_falls_back_to_the_bind() {
        let root = Path::new("/work/project");
        for stamp in [None, Some(stamped(None, None))] {
            let location = StampedProject::resolve(stamp.as_ref(), root, Some("bind-project"));
            assert_eq!(location.workdir, root);
            assert!(!location.workdir_stamped);
            assert_eq!(location.project_id, Some("bind-project"));
            assert_eq!(location.compare(root), WorkdirComparison::Absent);
        }
    }

    #[test]
    fn a_stamped_folder_and_project_take_precedence_over_the_bind() {
        let root = Path::new("/work/project");
        let stamp = stamped(Some("/work/other"), Some("pj-1"));
        let location = StampedProject::resolve(Some(&stamp), root, Some("bind-project"));
        assert_eq!(location.workdir, Path::new("/work/other"));
        assert_eq!(location.project_id, Some("pj-1"));
        assert_eq!(location.compare(root), WorkdirComparison::Differs);
        // The same folder spelled with a trailing separator is the same path.
        let same = stamped(Some("/work/project/"), None);
        let location = StampedProject::resolve(Some(&same), root, None);
        assert_eq!(location.compare(root), WorkdirComparison::Same);
    }

    #[test]
    fn the_stamp_fields_are_read_from_the_daemon_stamp() {
        let stamp = subc_protocol::scope::ScopeStamp::new(
            subc_protocol::Principal::Direct,
            "head-1",
            1,
            subc_protocol::scope::ScopeKind::Head,
        )
        .with_workdir(Some("/work/other".into()))
        .with_project_id(Some("pj-1".into()));
        assert_eq!(
            StampedProject::from_stamp(&stamp),
            stamped(Some("/work/other"), Some("pj-1"))
        );
    }

    #[test]
    fn each_session_is_counted_and_logged_once_per_outcome() {
        let tally = WorkdirTally::default();
        let session = BindIdentity::new("/work/project", "runner", "s1");
        let other = BindIdentity::new("/work/project", "runner", "s2");
        let differs = stamped(Some("/work/other"), None);
        let same = stamped(Some("/work/project"), None);

        assert!(observe_into(&tally, &session, Some(&differs), None));
        assert!(
            !observe_into(&tally, &session, Some(&differs), None),
            "a repeated send from the same session neither logs nor counts again"
        );
        assert!(!observe_into(&tally, &other, Some(&same), None));
        assert!(!observe_into(&tally, &other, None, None));
        assert!(!observe_into(&tally, &other, None, None));
        assert_eq!(
            tally.health(),
            serde_json::json!({
                "sessions_same": 1,
                "sessions_differ": 1,
                "sessions_without_workdir": 1,
                "sessions_untracked": 0,
            })
        );
    }
}
