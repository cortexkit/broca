//! Experiments a deployment turns on for chosen project folders, read from `experiments.json` in
//! the state root (the folder that holds `run-index.db`):
//!
//! ```json
//! {"ride_placement": {"sibling_project_roots": ["/absolute/project/folder"]}}
//! ```
//!
//! The file is read each time a run on the new session storage is admitted, so editing it needs
//! no restart, and only the runs admitted after the edit see it. A missing, unreadable, malformed
//! or not owner-only file selects nothing and never refuses a run; it is logged once as a warning
//! (again only when the reason changes).
//!
//! Today the file holds one probe: where a ride's text goes in Anthropic Messages requests (see
//! `broca_provider::wire::RidePlacement`). It exists to measure how models treat the two
//! placements and is removed or replaced once that question is decided.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use broca_provider::wire::RidePlacement;
use serde::Deserialize;

/// The file's name in the state root.
pub const FILE: &str = "experiments.json";

#[derive(Debug, Default, Deserialize)]
struct Experiments {
    #[serde(default)]
    ride_placement: Option<RidePlacementExperiment>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RidePlacementExperiment {
    /// The project folders whose sessions show rides as their own text blocks after the tool
    /// turn's results. Each must be absolute.
    sibling_project_roots: Vec<PathBuf>,
}

/// The last reason the file was not used, so that each reason is logged once.
static LAST_WARNING: Mutex<Option<String>> = Mutex::new(None);

/// The ride placement `experiments.json` in `state_root` selects for a run of a session in the
/// project folder `project_root`, or `None` when it selects none. Probe only: removed or
/// replaced once the placement question is decided. Whether the run's route can render the
/// placement is decided at freeze (only Anthropic Messages freezes it).
pub fn ride_placement(state_root: &Path, project_root: &Path) -> Option<RidePlacement> {
    let experiments = match read(&state_root.join(FILE)) {
        Ok(experiments) => {
            *LAST_WARNING
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = None;
            experiments
        }
        Err(reason) => {
            warn_once(reason);
            return None;
        }
    };
    experiments
        .ride_placement?
        .sibling_project_roots
        .iter()
        .any(|root| root == project_root)
        .then_some(RidePlacement::AfterResults)
}

fn warn_once(reason: String) {
    let mut last = LAST_WARNING
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if last.as_deref() != Some(reason.as_str()) {
        tracing::warn!(target: "experiments", "{reason}; no experiment is selected");
        *last = Some(reason);
    }
}

fn read(path: &Path) -> Result<Experiments, String> {
    check_owner_only(path)?;
    let bytes = std::fs::read(path)
        .map_err(|error| format!("{} can't be read: {error}", path.display()))?;
    let experiments: Experiments = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{} is malformed: {error}", path.display()))?;
    if let Some(relative) = experiments.ride_placement.as_ref().and_then(|experiment| {
        experiment
            .sibling_project_roots
            .iter()
            .find(|root| !root.is_absolute())
    }) {
        return Err(format!(
            "{} is malformed: the project folder {} is not absolute",
            path.display(),
            relative.display()
        ));
    }
    Ok(experiments)
}

#[cfg(unix)]
fn check_owner_only(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{} can't be read: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(format!(
            "{} is not owner-only (mode {mode:o})",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_owner_only(path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{} can't be read: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    broca_owner_only::validate(path)
        .map_err(|error| format!("{} is not owner-only: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(name: &str) -> PathBuf {
        let root = broca_test_support::scratch_root()
            .join(format!("experiments-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write(root: &Path, text: &str) {
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        use std::io::Write;
        options
            .open(root.join(FILE))
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
    }

    const LISTED: &str = r#"{"ride_placement": {"sibling_project_roots": ["/work/probe"]}}"#;

    /// A listed folder is selected, any other folder isn't, and each edit is seen by the next
    /// read without a restart.
    #[test]
    fn a_listed_project_folder_is_selected_and_edits_apply_at_once() {
        let root = root("listed");
        write(&root, LISTED);
        assert_eq!(
            ride_placement(&root, Path::new("/work/probe")),
            Some(RidePlacement::AfterResults)
        );
        assert_eq!(
            ride_placement(&root, Path::new("/work/probe/")),
            Some(RidePlacement::AfterResults)
        );
        assert_eq!(ride_placement(&root, Path::new("/work/other")), None);
        assert_eq!(ride_placement(&root, Path::new("/work")), None);
        write(
            &root,
            r#"{"ride_placement": {"sibling_project_roots": ["/work/other"]}}"#,
        );
        assert_eq!(ride_placement(&root, Path::new("/work/probe")), None);
        assert_eq!(
            ride_placement(&root, Path::new("/work/other")),
            Some(RidePlacement::AfterResults)
        );
        write(&root, "{}");
        assert_eq!(ride_placement(&root, Path::new("/work/other")), None);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A missing, malformed or relative-folder file selects nothing rather than failing.
    #[test]
    fn an_unusable_file_selects_nothing() {
        let root = root("unusable");
        assert_eq!(ride_placement(&root, Path::new("/work/probe")), None);
        for text in [
            "not json",
            r#"{"ride_placement": {"sibling_project_roots": "/work/probe"}}"#,
            r#"{"ride_placement": {"roots": ["/work/probe"]}}"#,
            r#"{"ride_placement": {"sibling_project_roots": ["work/probe"]}}"#,
        ] {
            write(&root, text);
            assert_eq!(
                ride_placement(&root, Path::new("/work/probe")),
                None,
                "{text}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A file others can read or write selects nothing.
    #[cfg(unix)]
    #[test]
    fn a_file_that_is_not_owner_only_selects_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let root = root("broad");
        write(&root, LISTED);
        std::fs::set_permissions(root.join(FILE), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(ride_placement(&root, Path::new("/work/probe")), None);
        std::fs::set_permissions(root.join(FILE), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            ride_placement(&root, Path::new("/work/probe")),
            Some(RidePlacement::AfterResults)
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
