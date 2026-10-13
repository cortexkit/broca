//! The whole-array prompt transform route stays removed.
//!
//! broca used to hand the entire prompt to a configured transform module before every model
//! request, armed by an environment variable and run through a subc-routed plane. The route was
//! removed outright, with no flag and no shim, in favour of per-step role contracts. Its old
//! names are the cheapest signal that it is creeping back: a revived environment variable or a
//! re-added plane would spell them again. This scan fails on either name anywhere in the code
//! (crates, scripts and CI), so bringing the route back is a decision someone has to make here,
//! not an accident nobody notices.
//!
//! Durable readers are deliberately NOT covered: `RunFinishReason::TransformUnavailable` and the
//! `{"transform": ...}` context-management value still decode, because old WALs carry them.
//!
//! The names are assembled with `concat!` so this file does not match its own scan.

use std::path::{Path, PathBuf};

/// The environment variable that armed the route, and the plane that ran it.
const REMOVED_NAMES: [&str; 2] = [
    concat!("BROCA_", "TRANSFORM_", "MODULE"),
    concat!("Subc", "Transform", "Plane"),
];

/// Every line of `text` naming a removed identifier, as `(1-based line, name)`.
fn removed_name_sites(text: &str) -> Vec<(usize, &'static str)> {
    let mut sites = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for name in REMOVED_NAMES {
            if line.contains(name) {
                sites.push((index + 1, name));
            }
        }
    }
    sites
}

fn code_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == "target" || name == "node_modules")
            {
                continue;
            }
            code_files(&path, out);
        } else if path.extension().is_some_and(|ext| {
            [
                "rs", "toml", "sh", "py", "ts", "yml", "yaml", "json", "jsonc",
            ]
            .iter()
            .any(|known| ext == *known)
        }) {
            out.push(path);
        }
    }
}

/// The scanner must be able to fail: a planted reference to either name is reported on its
/// line, and a line that merely mentions transforms is not.
#[test]
fn the_scan_flags_a_planted_reference_to_either_removed_name() {
    let planted = format!(
        "let a = std::env::var(\"{}\");\nlet transform = 1;\nlet b = broca_subc::{}::new();\n",
        REMOVED_NAMES[0], REMOVED_NAMES[1]
    );
    assert_eq!(
        removed_name_sites(&planted),
        [(1, REMOVED_NAMES[0]), (3, REMOVED_NAMES[1])]
    );
    assert!(removed_name_sites("let transform_unavailable = true;\n").is_empty());
}

#[test]
fn no_code_names_the_removed_transform_route() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for root in ["crates", "scripts", ".github"] {
        code_files(&workspace.join(root), &mut files);
    }
    let mut offenders = Vec::new();
    let mut scanned_rust = 0;
    for file in &files {
        let relative = file.strip_prefix(&workspace).unwrap_or(file);
        if file.extension().is_some_and(|ext| ext == "rs") {
            scanned_rust += 1;
        }
        let text = std::fs::read_to_string(file).unwrap_or_default();
        for (line, name) in removed_name_sites(&text) {
            offenders.push(format!("{}:{line}: {name}", relative.display()));
        }
    }
    // NON-VACUITY: a walk that reached nothing would pass while proving nothing, which is the
    // state a moved directory produces.
    assert!(
        scanned_rust > 200,
        "only {scanned_rust} Rust files scanned; the walk is not reading the workspace"
    );
    assert!(
        files
            .iter()
            .any(|file| file.ends_with("crates/broca-core/src/run.rs")),
        "the scan never reached the run loop, where the removed seam lived"
    );
    assert!(
        offenders.is_empty(),
        "the whole-array prompt transform route was removed; these lines name it again:\n  {}",
        offenders.join("\n  ")
    );
}
