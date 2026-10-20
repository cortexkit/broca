//! Operator-facing logs stay visible on an unconfigured daemon; wire diagnostics are opt-in.
//!
//! `cortexkit-log` defaults its root to INFO when `CK_LOG` is absent from the environment, and
//! the daemon injects `SUBC_MODULE_ID` without injecting `CK_LOG` unless `subc.jsonc` carries a
//! log block. So a site at `debug` or `trace` is not quiet, it is GONE -- with a green suite,
//! because a level is not a compile error. That is the whole reason the adoption landed as two
//! commits: every site moved to `warn` first, and the demotions are reviewed on their own.
//!
//! This is a source scan rather than a rendered-record assertion, deliberately. A record test
//! proves one site's level; the hazard is a site nobody thought about, which only a census over
//! all of them can see.
//!
//! THE OTHER HALF OF THE MIGRATION'S RISK, MEASURED AND EMPTY. `diagln!` wrote to stderr
//! unconditionally; `tracing::` needs a subscriber, so any migrated site reachable from a
//! binary that installs none went SILENT where it used to print. Four shipped binaries install
//! no subscriber -- `store-maintenance`, `wal-verify`, `ck-import`, `broca-run` -- and three of
//! them link crates that log through tracing. Checked for REACH rather than for linkage:
//!
//!   broca-run           0 store references, and every broca-core site is on the projection
//!                       path, which does not run without a store
//!   ck-import           0 store references
//!   store-maintenance   0 calls to the insert path where broca-store's four sites fire
//!   wal-verify          no edge to any crate that logs through tracing
//!
//! So nothing operator-facing was silenced. Recorded because the next reader meets the same
//! question and the answer is not visible from any one file -- and because it stops being true
//! the moment one of those binaries grows a store, which is a change nobody would connect to
//! logging.

use std::path::Path;

/// Levels at or above the default root. Anything else vanishes on an unconfigured daemon.
const VISIBLE: [&str; 3] = ["warn", "info", "error"];

// These targets are opt-in diagnostics: the wire milestones help locate a stall, the retention
// enrollment event counts retryable races that are not operator alerts, and request records
// show which bound sessions still poll. Losing them at INFO hides no information that requires
// immediate action. Only exact literal targets at DEBUG qualify; TRACE and every other target
// stay refused, so this exception cannot silently demote ordinary operational logs.
const DIAGNOSTIC_TARGETS: [&str; 6] = [
    "wire.describe",
    "wire.ingress",
    "wire.egress",
    "subc.wire",
    "retention.enrollment",
    "requests",
];

fn diagnostic_debug_site(invocation: &str) -> bool {
    let Some(arguments) = invocation.strip_prefix("debug!") else {
        return false;
    };
    let Some(arguments) = arguments.trim_start().strip_prefix('(') else {
        return false;
    };
    let Some(target) = arguments.trim_start().strip_prefix("target:") else {
        return false;
    };
    let Some(target) = target.trim_start().strip_prefix('"') else {
        return false;
    };
    let Some((target, rest)) = target.split_once('"') else {
        return false;
    };
    rest.trim_start().starts_with(',') && DIAGNOSTIC_TARGETS.contains(&target)
}

fn scan_levels(text: &str) -> (usize, Vec<(usize, &str)>) {
    let mut sites = 0;
    let mut below = Vec::new();
    let mut offset = 0;
    for (index, line) in text.split_inclusive('\n').enumerate() {
        let line_start = offset;
        offset += line.len();
        let Some((prefix, rest)) = line.split_once("tracing::") else {
            continue;
        };
        let Some((level, _)) = rest.split_once('!') else {
            continue;
        };
        if !["warn", "info", "error", "debug", "trace"].contains(&level) {
            continue;
        }
        sites += 1;
        let invocation = &text[line_start + prefix.len() + "tracing::".len()..];
        if !VISIBLE.contains(&level) && !diagnostic_debug_site(invocation) {
            below.push((index + 1, level));
        }
    }
    (sites, below)
}

#[test]
fn ordinary_debug_and_trace_sites_stay_refused() {
    let (sites, below) =
        scan_levels("tracing::debug!(\"ordinary\");\ntracing::trace!(\"ordinary\");\n");
    assert_eq!(sites, 2);
    assert_eq!(below, [(1, "debug"), (2, "trace")]);
}

#[test]
fn diagnostic_debug_exception_rejects_unlisted_targets() {
    let (sites, below) =
        scan_levels("tracing::debug!(\n    target: \"wire.unlisted\", \"ordinary\"\n);\n");
    assert_eq!(sites, 1);
    assert_eq!(below, [(1, "debug")]);
}

#[test]
fn diagnostic_debug_exception_is_not_a_prefix_or_trace_exception() {
    for invocation in [
        "debug!(target: \"wire.describe.extra\", \"ordinary\");",
        "debug!(target: \"wire.ingress_extra\", \"ordinary\");",
        "debug!(target: \"wire.egress.extra\", \"ordinary\");",
        "debug!(target: \"subc.wire.extra\", \"ordinary\");",
        "trace!(target: \"wire.describe\", \"ordinary\");",
        "debug!(target: dynamic_target, \"ordinary\");",
        "debug!(\"ordinary\");\ntracing::debug!(target: \"wire.describe\", \"diagnostic\");",
    ] {
        assert!(
            !diagnostic_debug_site(invocation),
            "unapproved exception: {invocation}"
        );
    }
    for target in DIAGNOSTIC_TARGETS {
        let invocation = format!("debug!(\n    target: \"{target}\", \"diagnostic\"\n);");
        assert!(
            diagnostic_debug_site(&invocation),
            "approved diagnostic target: {target}"
        );
    }
}

fn shipped_sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read the crates tree") {
        let path = entry.expect("read a directory entry").path();
        if path.is_dir() {
            // `src` only: a test may legitimately emit at debug, and benches are not shipped.
            if path
                .file_name()
                .is_some_and(|n| n == "tests" || n == "benches")
            {
                continue;
            }
            shipped_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs")
            && path.components().any(|c| c.as_os_str() == "src")
        {
            out.push(path);
        }
    }
}

#[test]
fn no_operator_log_site_sits_below_the_unconfigured_default() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crates directory");
    let mut sources = Vec::new();
    shipped_sources(crates, &mut sources);

    let mut sites = 0usize;
    let mut below = Vec::new();
    for path in &sources {
        let text = std::fs::read_to_string(path).expect("read a shipped source file");
        let (count, refused) = scan_levels(&text);
        sites += count;
        for (line, level) in refused {
            below.push(format!("{}:{line} at {level}", path.display()));
        }
    }

    // NON-VACUITY: a scan that found no sites would pass this test while proving nothing, and
    // that is the state a moved directory or a renamed macro produces.
    assert!(
        sites >= 60,
        "the scan found only {sites} log sites across {} files; it is not reading the tree it \
         thinks it is, and its silence about levels means nothing",
        sources.len()
    );

    assert!(
        below.is_empty(),
        "these sites are below the INFO root and are not approved opt-in wire diagnostics, \
         so they hide operational information -- raise them: {below:#?}"
    );
    println!(
        "log-level census: {sites} sites in {} source files",
        sources.len()
    );
}
