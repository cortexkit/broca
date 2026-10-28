// A `src/bin/` target is its OWN crate root and does NOT inherit the sibling lib/main
// attribute, so this must be stated per binary or the guarantee silently does not apply here.
#![forbid(unsafe_code)]

//! Explicit maintenance entry point for derived materialized-store metadata.
//!
//! Usage:
//!   store-maintenance page-usage <state-root> [--dry-run]
//!   store-maintenance control-head <state-root> [--dry-run]
//!   store-maintenance private-mode <state-root> [--dry-run]
//!   store-maintenance wal-manifest <state-root> [--write]

use std::path::PathBuf;
use std::process::ExitCode;

use broca_core::FileWalStore;
use broca_store::SqliteStore;

/// Why a state root is unusable for an operation, or `None` when it is usable.
///
/// Extracted from `main` so the refusals are reachable by a test. Inline they were enforced by
/// two `if` statements inside an entry point nothing can call, which is the arrangement where a
/// later edit can weaken a guard and every test still passes.
///
/// THE POINT OF REFUSING RATHER THAN PROCEEDING: this tool repairs the production store, and
/// both failure modes below would otherwise produce a LEGAL, PLAUSIBLE verdict about nothing --
/// `declined <n>` and exit 1, which reads as "these rows are unrecoverable". An operator can fix
/// a mistyped path and cannot fix a wrong conclusion they never saw.
fn state_root_refusal(
    operation: &str,
    state_root: &std::path::Path,
    database_is_file: bool,
    wal_dir_exists: bool,
) -> Option<String> {
    if !state_root.is_dir() || !database_is_file {
        return Some(format!(
            "store-maintenance: {} must name a state root containing run-index.db",
            state_root.display()
        ));
    }
    // Control-head reads through FileWalStore, including verified archived members.
    // A fully folded tree need not retain its empty live directory.
    // reconcile-active reads the same way and would otherwise report every row as "declined"
    // -- a plausible verdict about a mistyped path.
    if matches!(operation, "control-head" | "reconcile-active")
        && !wal_dir_exists
        && !state_root.join("wal-archive").is_dir()
        && !state_root.join("wal-archive-durable").is_dir()
    {
        return Some(format!(
            "store-maintenance: {} has no wal/ directory, so every row would decline for a \
             missing input rather than an underivable cursor. Point at a state root whose \
             WALs are present.",
            state_root.display()
        ));
    }
    if operation == "wal-manifest" && !wal_dir_exists {
        return Some(format!(
            "store-maintenance: {} has no wal/ directory, so there is no WAL tree to report or \
             rebuild",
            state_root.display()
        ));
    }
    None
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (Some(operation), Some(state_root)) = (args.get(1), args.get(2)) else {
        print_usage();
        return ExitCode::from(2);
    };
    if operation == "wal-fold" {
        return wal_fold(&PathBuf::from(state_root), &args[3..]);
    }
    if operation == "media-sweep" {
        if !(args.len() == 3 || args.len() == 4 && args[3] == "--write")
            || !std::path::Path::new(state_root).is_dir()
        {
            eprintln!("usage: store-maintenance media-sweep <existing-state-root> [--write]");
            return ExitCode::from(2);
        }
        let write = args.len() == 4;
        let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(now) => now.as_secs(),
            Err(error) => {
                eprintln!("store-maintenance: upload clock failed: {error}");
                return ExitCode::from(2);
            }
        };
        let uploads_removed =
            match broca_core::media_uploads::Uploads::at(std::path::Path::new(state_root))
                .sweep(now, write)
            {
                Ok(count) => count,
                Err(error) => {
                    eprintln!("store-maintenance: upload sweep failed: {error}");
                    return ExitCode::from(2);
                }
            };
        let result = if write {
            broca_core::media_store::sweep(std::path::Path::new(state_root))
        } else {
            broca_core::media_store::preview_sweep(std::path::Path::new(state_root))
        };
        return match result {
            Ok(report) => {
                println!("uploads_removed {uploads_removed}");
                println!(
                    "mode {}\nleased {}\nreconciled {}\nmanifests_removed {}\nobjects_removed {}\nwal_reads {}\nretained_classes {}",
                    if write { "write" } else { "preview" },
                    report.leased,
                    report.reconciled,
                    report.manifests_removed,
                    report.objects_removed,
                    report.wal_reads,
                    report.retained_classes,
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("store-maintenance: media-sweep failed: {error}");
                ExitCode::from(2)
            }
        };
    }
    let (dry_run, manifest_write) = match (operation.as_str(), args.get(3).map(String::as_str)) {
        ("wal-manifest", None) => (true, false),
        ("wal-manifest", Some("--write")) => (false, true),
        (_, None) => (false, false),
        (_, Some("--dry-run")) => (true, false),
        (_, Some(other)) => {
            eprintln!("store-maintenance: unknown option '{other}'");
            print_usage();
            return ExitCode::from(2);
        }
    };
    if args.len() > 4 {
        print_usage();
        return ExitCode::from(2);
    }

    let state_root = PathBuf::from(state_root);
    let database = state_root.join("run-index.db");
    if let Some(refusal) = state_root_refusal(
        operation,
        &state_root,
        database.is_file(),
        [
            "wal",
            "wal-ephemeral",
            "wal-durable",
            "wal-archive",
            "wal-archive-durable",
        ]
        .iter()
        .any(|name| state_root.join(name).is_dir()),
    ) {
        eprintln!("{refusal}");
        return ExitCode::from(2);
    }
    if operation == "private-mode" {
        if !dry_run {
            let liveness = writer_liveness(&state_root);
            if liveness.state != "no" {
                eprintln!(
                    "store-maintenance: refusing offline permission repair: {} ({})",
                    liveness.state, liveness.reason
                );
                return ExitCode::from(3);
            }
        }
        return private_mode(&state_root, dry_run);
    }
    if operation == "wal-manifest" {
        return wal_manifest(&state_root, manifest_write);
    }
    let liveness = if dry_run {
        WriterLiveness::stopped()
    } else {
        writer_liveness(&state_root)
    };
    let store = match open_maintenance_store(&state_root, dry_run, liveness) {
        Ok(store) => store,
        Err(code) => return code,
    };

    match operation.as_str() {
        "page-usage" => {
            let result = if dry_run {
                store.preview_page_usage_backfill()
            } else {
                store.backfill_page_usage()
            };
            let counts = match result {
                Ok(counts) => counts,
                Err(error) => {
                    eprintln!("store-maintenance: page-usage backfill failed: {error}");
                    return ExitCode::from(2);
                }
            };
            println!("scanned {}", counts.scanned);
            println!("converted {}", counts.converted);
            println!("empty {}", counts.empty);
            println!("declined {}", counts.declined);
            println!(
                "total {}",
                counts.converted + counts.empty + counts.declined
            );
            exit_for_declines(counts.declined)
        }
        "reconcile-active" => {
            let wal_store = FileWalStore::new(state_root.join("wal"));
            let result = if dry_run {
                store.preview_active_row_reconcile(&wal_store)
            } else {
                store.reconcile_active_rows(&wal_store)
            };
            let counts = match result {
                Ok(counts) => counts,
                Err(error) => {
                    eprintln!("store-maintenance: reconcile-active failed: {error}");
                    return ExitCode::from(2);
                }
            };
            println!("scanned {}", counts.scanned);
            println!("reconciled {}", counts.reconciled);
            // Not a decline: the WAL agrees with the row that nothing ended the run. Printed
            // so an operator can tell "abandoned, will seal on next append" from "stale".
            println!("unsealed {}", counts.unsealed);
            println!("declined {}", counts.declined);
            println!(
                "total {}",
                counts.reconciled + counts.unsealed + counts.declined
            );
            exit_for_declines(counts.declined)
        }
        "control-head" => {
            // FileWalStore resolves both live and archived WALs. A direct live-path
            // check here would misclassify folded rows as permanently declined_no_wal.
            let wal_store = FileWalStore::new(state_root.join("wal"));
            let result = if dry_run {
                store.preview_control_head_repair(&wal_store)
            } else {
                store.repair_control_heads(&wal_store)
            };
            let counts = match result {
                Ok(counts) => counts,
                Err(error) => {
                    eprintln!("store-maintenance: control-head repair failed: {error}");
                    return ExitCode::from(2);
                }
            };
            println!("scanned {}", counts.scanned);
            println!("repaired {}", counts.repaired);
            println!("declined {}", counts.declined);
            // Named separately because it is the PERMANENT part: no log exists, so no future
            // run of this repair will fix those rows. An operator seeing a non-zero decline
            // count otherwise cannot tell finished business from work still to do.
            println!("declined_no_wal {}", counts.declined_no_wal);
            println!("total {}", counts.repaired + counts.declined);
            exit_for_declines(counts.declined)
        }
        other => {
            eprintln!("store-maintenance: unknown operation '{other}'");
            print_usage();
            ExitCode::from(2)
        }
    }
}

/// Exit 1 when any row declined, so a partial repair cannot read as a complete one.
///
/// NOTE ON THE STEADY STATE, measured after both repairs ran against production: the
/// remaining declines are permanently undecidable -- 16 control-head rows whose WAL is gone,
/// 1,147 page-usage rows with no admission checkpoint -- so every future run exits 1 on rows
/// nothing can fix. That is a weak signal by design, and it is still the right one: the
/// alternative (exit 0 once the repairable backlog drains) requires distinguishing "declined
/// because undecidable" from "declined because something went wrong", and both currently
/// increment the same counter. An attempt to key the exit on scanned-vs-accounted was
/// reverted because every row increments exactly one bucket, making the two equal by
/// construction -- a check that could never fire.
///
/// If the distinction is ever wanted, it needs a separate counter at the decline sites, not a
/// cleverer predicate over the existing ones.
fn exit_for_declines(declined: u64) -> ExitCode {
    if declined == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn print_usage() {
    eprintln!(
        "usage: store-maintenance page-usage|control-head|reconcile-active|private-mode <state-root> [--dry-run]\n\
                 store-maintenance wal-manifest <state-root> [--write]\n\
                 store-maintenance media-sweep <state-root> [--write]\n\
                 store-maintenance wal-fold <state-root> [--write] [--resume fold-<unix-ms>.journal] [--abandon|--unfold]"
    );
}

struct ExistingManifest {
    lines: usize,
    addresses: std::collections::BTreeSet<String>,
}

fn wal_addresses(wal_dir: &std::path::Path) -> std::io::Result<std::collections::BTreeSet<String>> {
    let mut addresses = std::collections::BTreeSet::new();
    for name in ["wal", "wal-ephemeral", "wal-durable"] {
        let directory = wal_dir
            .parent()
            .unwrap_or_else(|| std::path::Path::new(""))
            .join(name);
        let entries = match std::fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("wal") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("WAL filename is not UTF-8: {}", path.display()),
                ));
            };
            addresses.insert(stem.to_owned());
        }
    }
    Ok(addresses)
}

fn parsed_manifest_address(line: &str) -> Option<&str> {
    let mut fields = line.splitn(3, "  ");
    let address = fields.next()?;
    let lineage = fields.next()?;
    let identity = fields.next()?;
    let address_is_valid =
        address.len() == 16 && address.bytes().all(|byte| byte.is_ascii_hexdigit());
    let lineage_is_valid = lineage.len() == 36
        && lineage.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    let identity_is_valid = identity.split('\u{1f}').count() == 3;
    (address_is_valid && lineage_is_valid && identity_is_valid).then_some(address)
}

fn existing_manifest(path: &std::path::Path) -> std::io::Result<ExistingManifest> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error),
    };
    let text = String::from_utf8_lossy(&bytes);
    Ok(ExistingManifest {
        lines: text.lines().count(),
        addresses: text
            .lines()
            .filter_map(parsed_manifest_address)
            .map(str::to_owned)
            .collect(),
    })
}

fn replace_manifest(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp = path.with_file_name(format!(
        ".wal-manifest.tmp-{}-{counter}",
        std::process::id()
    ));
    let result = (|| {
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&temp)?;
        file.write_all(contents.as_bytes())?;
        drop(file);
        std::fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

#[derive(serde::Deserialize)]
struct WriterLiveness {
    state: String,
    reason: String,
}

impl WriterLiveness {
    fn stopped() -> Self {
        Self {
            state: "no".into(),
            reason: "known stopped".into(),
        }
    }

    fn unknown(reason: String) -> Self {
        Self {
            state: "unknown".into(),
            reason,
        }
    }
}

/// Embed the shared Python probe so installed maintenance binaries do not depend
/// on a source checkout. Missing Python, an unreadable process, and malformed
/// probe output all mean unknown, never permission to initialise SQLite.
fn writer_liveness(state_root: &std::path::Path) -> WriterLiveness {
    let script = format!(
        "import sys, types\nlib = types.ModuleType('lib')\nsys.modules['lib'] = lib\n\
         lib.state_root = types.ModuleType('lib.state_root')\n\
         exec({}, lib.state_root.__dict__)\nexec({})\n",
        serde_json::to_string(include_str!("../../../../scripts/lib/state_root.py")).unwrap(),
        serde_json::to_string(include_str!("../../../../scripts/lib/liveness.py")).unwrap(),
    );
    let python = if cfg!(windows) { "python" } else { "python3" };
    let output = match std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(state_root)
        .output()
    {
        Ok(output) => output,
        Err(error) => return WriterLiveness::unknown(format!("cannot run shared probe: {error}")),
    };
    if let Ok(probe) = serde_json::from_slice::<WriterLiveness>(&output.stdout) {
        let expected = match probe.state.as_str() {
            "no" => 0,
            "yes" => 3,
            "unknown" => 4,
            _ => return WriterLiveness::unknown("invalid shared probe state".into()),
        };
        if output.status.code() == Some(expected) {
            return probe;
        }
    }
    WriterLiveness::unknown(format!(
        "shared probe failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn open_maintenance_store(
    state_root: &std::path::Path,
    read_only: bool,
    liveness: WriterLiveness,
) -> Result<SqliteStore, ExitCode> {
    if !read_only && liveness.state != "no" {
        // This wording is the shared liveness contract (scripts/lib/liveness.py prints the same
        // line for the Python tools, and both suites match its prefix), so the state root goes
        // after the reason rather than inside the fixed phrase.
        eprintln!(
            "store-maintenance: liveness {}; refusing offline mutation: {} (state root {}). Stop the daemon first.",
            liveness.state,
            liveness.reason,
            state_root.display()
        );
        return Err(ExitCode::from(3));
    }
    let database = state_root.join("run-index.db");
    let result = if read_only {
        SqliteStore::open_read_only(&database)
    } else {
        SqliteStore::open(&database)
    };
    result.map_err(|error| {
        eprintln!(
            "store-maintenance: cannot open {}: {error}",
            database.display()
        );
        ExitCode::from(2)
    })
}

/// Report manifest coverage and per-session project-id observations by default; replace
/// the sidecar only after an explicit `--write`. Observations belong in the report, never
/// in sidecar mapping lines: the reader cannot distinguish a tab-separated suffix from
/// a session name containing a tab. Live and rebuilt mappings use the same identity encoding.
fn wal_manifest(state_root: &std::path::Path, write: bool) -> ExitCode {
    let liveness = if write {
        writer_liveness(state_root)
    } else {
        WriterLiveness::stopped()
    };
    wal_manifest_with(state_root, write, liveness)
}

/// Inject the probe result to test refusal order independently of the host's daemons.
fn wal_manifest_with(
    state_root: &std::path::Path,
    write: bool,
    liveness: WriterLiveness,
) -> ExitCode {
    let store = match open_maintenance_store(state_root, !write, liveness) {
        Ok(store) => store,
        Err(code) => return code,
    };
    let wal_store = FileWalStore::new(state_root.join("wal"));
    let rebuild = match store.rebuild_wal_manifest(&wal_store) {
        Ok(rebuild) => rebuild,
        Err(error) => {
            eprintln!("store-maintenance: cannot derive complete WAL manifest: {error}");
            return ExitCode::from(2);
        }
    };
    let wal_addresses = match wal_addresses(&state_root.join("wal")) {
        Ok(addresses) => addresses,
        Err(error) => {
            eprintln!("store-maintenance: cannot enumerate WAL tree: {error}");
            return ExitCode::from(2);
        }
    };
    let manifest_path = wal_store.manifest_path();
    let existing = match existing_manifest(&manifest_path) {
        Ok(existing) => existing,
        Err(error) => {
            eprintln!(
                "store-maintenance: cannot read {}: {error}",
                manifest_path.display()
            );
            return ExitCode::from(2);
        }
    };
    let missing_existing = wal_addresses.difference(&existing.addresses).count();
    let not_rebuildable: Vec<_> = wal_addresses.difference(&rebuild.addresses).collect();

    println!("wals {}", wal_addresses.len());
    println!("manifest_lines {}", existing.lines);
    println!("wals_without_manifest_line {missing_existing}");
    println!("rebuild_lines {}", rebuild.addresses.len());
    println!("sessions_without_wal {}", rebuild.sessions_without_wal);
    for (address, observed) in &rebuild.project_observations {
        println!(
            "project_observations {address} {}",
            serde_json::to_string(observed).expect("observation JSON")
        );
    }

    if !write {
        return ExitCode::SUCCESS;
    }
    if !not_rebuildable.is_empty() {
        eprintln!(
            "store-maintenance: refusing to replace {}: {} WAL(s) have no session row, including {}",
            manifest_path.display(),
            not_rebuildable.len(),
            not_rebuildable[0]
        );
        return ExitCode::from(2);
    }
    if let Err(error) = replace_manifest(&manifest_path, &rebuild.contents) {
        eprintln!(
            "store-maintenance: cannot replace {}: {error}",
            manifest_path.display()
        );
        return ExitCode::from(2);
    }
    println!("written {}", rebuild.addresses.len());
    ExitCode::SUCCESS
}

/// Tighten an existing state root to owner-only.
///
/// New logs are created 0600 under 0700 directories by the WAL writer, but a tree created
/// before that change keeps whatever the umask gave it -- 0644 under 0755 on a default
/// install. That is a one-time repair over every session file, which is why it lives here
/// rather than in the writer's open path: a filesystem walk has no business on the durability
/// path, and an operator needs to be able to run it once and see what it touched.
///
/// Reports files and directories separately because they fail differently: a private file
/// under a traversable directory still leaks its name, size and mtime, and a private
/// directory containing group-readable files is safe only until something re-opens the tree.
///
/// THE WALK DESCENDS ONLY. The state root is the leaf this module owns; its parent
/// (`~/.local/share/cortexkit/`) is shared with every other fleet module, and past the leaf a
/// mode is not ours to set. Sealing the ancestors would make the tree unreachable rather than
/// merely private, which reads as a strictly stronger fix and would break every sibling
/// writing under the same parent. The current code is correct because it starts at the root it
/// was handed and never ascends -- a property worth stating, since the tempting extension is
/// exactly the one that reaches too far.
#[cfg(unix)]
fn private_mode(state_root: &std::path::Path, dry_run: bool) -> ExitCode {
    use std::os::unix::fs::PermissionsExt;

    let mut scanned_dirs = 0u64;
    let mut scanned_files = 0u64;
    let mut tightened_dirs = 0u64;
    let mut tightened_files = 0u64;
    let mut failed = 0u64;

    let mut stack = vec![state_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        scanned_dirs += 1;
        match std::fs::metadata(&dir) {
            Ok(meta) if meta.permissions().mode() & 0o077 != 0 => {
                tightened_dirs += 1;
                if !dry_run {
                    if let Err(error) =
                        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                    {
                        eprintln!(
                            "store-maintenance: cannot tighten {}: {error}",
                            dir.display()
                        );
                        failed += 1;
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("store-maintenance: cannot stat {}: {error}", dir.display());
                failed += 1;
                continue;
            }
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                eprintln!("store-maintenance: cannot read {}: {error}", dir.display());
                failed += 1;
                continue;
            }
        };
        for entry in entries {
            let Ok(entry) = entry else {
                failed += 1;
                continue;
            };
            let path = entry.path();
            // symlink_metadata, not metadata: a symlink out of the tree must not be followed,
            // and chmod through one would tighten whatever it points at.
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                failed += 1;
                continue;
            };
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                scanned_files += 1;
                if meta.permissions().mode() & 0o077 != 0 {
                    tightened_files += 1;
                    if !dry_run {
                        if let Err(error) =
                            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                        {
                            eprintln!(
                                "store-maintenance: cannot tighten {}: {error}",
                                path.display()
                            );
                            failed += 1;
                        }
                    }
                }
            }
        }
    }

    println!("scanned dirs {scanned_dirs}");
    println!("scanned files {scanned_files}");
    println!(
        "{} dirs {tightened_dirs}",
        if dry_run {
            "would tighten"
        } else {
            "tightened"
        }
    );
    println!(
        "{} files {tightened_files}",
        if dry_run {
            "would tighten"
        } else {
            "tightened"
        }
    );
    println!("failed {failed}");

    // The vacuity guard has to be on the WAL tree, not on the file count. A `scanned_files == 0`
    // check reads like the right defence and cannot fire: the state-root validation above
    // already requires run-index.db to be a file, so the scan always finds at least one. The
    // reachable version of that mistake is a state root whose wal/ is absent or empty -- the
    // scan then tightens the database, reports success, and leaves every session log exactly as
    // it was, which is where all the prompt text lives.
    let wal_dir = state_root.join("wal");
    let wal_files: usize = [
        "wal",
        "wal-durable",
        "wal-ephemeral",
        "wal-archive",
        "wal-archive-durable",
    ]
    .iter()
    .map(|name| {
        std::fs::read_dir(state_root.join(name))
            .map(|entries| entries.filter_map(Result::ok).count())
            .unwrap_or(0)
    })
    .sum();
    if wal_files == 0 {
        eprintln!(
            "store-maintenance: {} holds no session logs, so this run would report success \
             having repaired only the database. Point at a state root whose wal/ is populated.",
            wal_dir.display()
        );
        return ExitCode::from(2);
    }
    if failed > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(windows)]
fn private_mode(state_root: &std::path::Path, dry_run: bool) -> ExitCode {
    private_mode_acl(
        state_root,
        dry_run,
        &mut std::io::stderr(),
        #[cfg(test)]
        None,
    )
}

#[cfg(any(windows, test))]
fn private_mode_acl(
    state_root: &std::path::Path,
    dry_run: bool,
    errors: &mut impl std::io::Write,
    #[cfg(test)] denied_acl: Option<&std::path::Path>,
) -> ExitCode {
    let mut scanned_dirs = 0u64;
    let mut scanned_files = 0u64;
    let mut tightened = 0u64;
    let mut failed = 0u64;
    let mut stack = vec![state_root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!(
                    "store-maintenance: cannot inspect {}: {error}",
                    path.display()
                );
                failed += 1;
                continue;
            }
        };
        if broca_owner_only::is_reparse(&metadata) {
            eprintln!(
                "store-maintenance: refusing reparse point {}",
                path.display()
            );
            failed += 1;
            continue;
        }
        if metadata.is_dir() {
            scanned_dirs += 1;
        } else if metadata.is_file() {
            scanned_files += 1;
        } else {
            eprintln!("store-maintenance: unsupported object {}", path.display());
            failed += 1;
            continue;
        }
        // A privileged Windows token can read the ACL despite a deny ACE. The
        // test-only path override makes access denial independent of token privileges.
        #[cfg(test)]
        let inspection = if denied_acl == Some(path.as_path()) {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected ACL inspection denial",
            ))
        } else {
            #[cfg(windows)]
            {
                broca_owner_only::is_private(&path)
            }
            // Non-Windows hosts exercise the Windows walk with readable ACLs for
            // every path except the injected denial; they have no Windows ACL API.
            #[cfg(not(windows))]
            {
                Ok(true)
            }
        };
        #[cfg(not(test))]
        let inspection = broca_owner_only::is_private(&path);
        let private = match inspection {
            Ok(private) => private,
            Err(error) => {
                writeln!(
                    errors,
                    "store-maintenance: cannot inspect ACL {}: {error}",
                    path.display()
                )
                .expect("write ACL inspection refusal");
                failed += 1;
                continue;
            }
        };
        if !private {
            if dry_run {
                println!("would tighten {}", path.display());
            } else {
                #[cfg(windows)]
                let repaired = broca_owner_only::repair(&path);
                #[cfg(all(test, not(windows)))]
                let repaired: std::io::Result<()> = Ok(());
                if let Err(error) = repaired {
                    eprintln!(
                        "store-maintenance: cannot tighten {}: {error}",
                        path.display()
                    );
                    failed += 1;
                } else {
                    tightened += 1;
                }
            }
        }
        if metadata.is_dir() {
            match std::fs::read_dir(&path) {
                Ok(entries) => {
                    for entry in entries {
                        match entry {
                            Ok(entry) => stack.push(entry.path()),
                            Err(error) => {
                                eprintln!(
                                    "store-maintenance: cannot inspect child of {}: {error}",
                                    path.display()
                                );
                                failed += 1;
                            }
                        }
                    }
                }
                Err(error) => {
                    eprintln!("store-maintenance: cannot read {}: {error}", path.display());
                    failed += 1;
                }
            }
        }
    }
    println!("scanned dirs {scanned_dirs}\nscanned files {scanned_files}\ntightened {tightened}\nfailed {failed}");
    if failed > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(not(any(unix, windows)))]
fn private_mode(_state_root: &std::path::Path, _dry_run: bool) -> ExitCode {
    eprintln!("store-maintenance: private-mode is a unix file-mode repair and has no effect here");
    ExitCode::from(2)
}

fn wal_fold(root: &std::path::Path, args: &[String]) -> ExitCode {
    if let Err(error) = broca_core::wal_fold::ensure_legacy_root(root) {
        eprintln!("store-maintenance: wal-fold refused: {error}");
        return ExitCode::from(2);
    }
    let mut options = broca_core::wal_fold::FoldOptions::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--write" if !options.write => options.write = true,
            "--abandon" if !options.abandon => options.abandon = true,
            "--unfold" if !options.unfold => options.unfold = true,
            "--resume" if options.resume.is_none() => {
                let Some(name) = args.next() else {
                    print_usage();
                    return ExitCode::from(2);
                };
                options.resume = Some(name.clone());
            }
            _ => {
                print_usage();
                return ExitCode::from(2);
            }
        }
    }
    let identities = match broca_store::fold_identities(root) {
        Ok(identities) => identities,
        Err(error) => {
            eprintln!("store-maintenance: cannot read fold identities: {error}");
            return ExitCode::from(2);
        }
    };
    match broca_core::wal_fold::fold_with_identities(root, &options, &identities) {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serializable fold report")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("store-maintenance: wal-fold refused: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::open_maintenance_store;
    #[test]
    fn reports_and_previews_open_read_only_without_schema_initialization() {
        let root = temp_root("read-only-maintenance");
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let db = root.join("run-index.db");
        drop(broca_store::SqliteStore::open(&db).unwrap());
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("DROP INDEX message_unattributed_session_idx", [])
            .unwrap();
        drop(conn);
        let before = std::fs::read(&db).unwrap();
        let store = open_maintenance_store(
            &root,
            true,
            WriterLiveness {
                state: "yes".into(),
                reason: "live writer".into(),
            },
        )
        .ok()
        .unwrap();
        let wal = FileWalStore::new(root.join("wal"));
        store.preview_page_usage_backfill().unwrap();
        store.preview_control_head_repair(&wal).unwrap();
        store.preview_active_row_reconcile(&wal).unwrap();
        drop(store);
        assert_eq!(
            wal_manifest_with(
                &root,
                false,
                WriterLiveness::unknown("cannot inspect process".into())
            ),
            ExitCode::SUCCESS
        );
        assert_eq!(
            std::fs::read(&db).unwrap(),
            before,
            "reports must not initialize or migrate the database"
        );
        let conn =
            rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let initialized: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='message_unattributed_session_idx')", [], |row| row.get(0)).unwrap();
        assert!(!initialized, "report opens must not run init");
    }

    #[test]
    fn every_mutating_maintenance_open_refuses_live_or_unknown_before_sqlite() {
        for state in ["yes", "unknown"] {
            let root = temp_root(&format!("maintenance-{state}"));
            std::fs::create_dir_all(&root).unwrap();
            let result = open_maintenance_store(
                &root,
                false,
                WriterLiveness {
                    state: state.into(),
                    reason: "injected liveness".into(),
                },
            );
            assert!(matches!(result, Err(code) if code == ExitCode::from(3)));
            assert!(
                !root.join("run-index.db").exists(),
                "liveness refusal must precede mutating SQLite open"
            );
        }
    }

    #[test]
    fn manifest_rebuild_resolves_all_live_roots_and_refuses_conflicts() {
        use broca_store::SqliteStore;
        use broca_wal::{FileWal, Wal, WalRecord};
        let root = temp_root("manifest-live-roots");
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        let wal_store = FileWalStore::new(root.join("wal"));
        for (i, directory) in ["wal", "wal-ephemeral", "wal-durable"]
            .into_iter()
            .enumerate()
        {
            let session = BindIdentity::new("/project", "test", directory);
            index_session(&store, &format!("run-{i}"), session.clone());
            let ordinary = wal_store.wal_path(&session);
            let path = root.join(directory).join(ordinary.file_name().unwrap());
            let mut wal = FileWal::open(path).unwrap();
            wal.append(
                1,
                &WalRecord::StepStarted {
                    step_id: broca_protocol::StepId(1),
                },
                true,
            )
            .unwrap();
        }
        let rebuilt = store.rebuild_wal_manifest(&wal_store).unwrap();
        assert_eq!(rebuilt.addresses.len(), 3);
        assert_eq!(rebuilt.sessions_without_wal, 0);
        drop(store);
        assert_eq!(
            wal_manifest_with(&root, true, WriterLiveness::stopped()),
            ExitCode::SUCCESS
        );
        assert_eq!(
            std::fs::read_to_string(wal_store.manifest_path())
                .unwrap()
                .lines()
                .count(),
            3
        );
        let session = BindIdentity::new("/project", "test", "wal-durable");
        FileWal::open(wal_store.wal_path(&session)).unwrap();
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        assert!(
            store.rebuild_wal_manifest(&wal_store).is_err(),
            "conflicting placements must not become an arbitrary manifest mapping"
        );
    }

    use super::{state_root_refusal, wal_manifest_with, WriterLiveness};
    use broca_core::{
        session_addr, FileWalStore, RunIndex, RunIndexRow, RunIndexState, WalStore,
        WAL_MANIFEST_FILENAME,
    };
    use broca_protocol::{BindIdentity, StepId};
    use broca_wal::{WalRecord, DEFAULT_MAX_READ_BYTES};
    use std::path::{Path, PathBuf};
    use std::process::ExitCode;

    fn temp_root(tag: &str) -> PathBuf {
        broca_test_support::scratch_root()
            .join(format!("store-maintenance-{}-{tag}", std::process::id()))
    }

    #[test]
    fn private_mode_refuses_injected_unreadable_acl() {
        let root = temp_root("unreadable-acl");
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let db = root.join("run-index.db");
        let file = root.join("wal").join("session.wal");
        std::fs::write(&db, b"database marker").unwrap();
        std::fs::write(&file, b"WAL marker").unwrap();
        for dry_run in [true, false] {
            let mut errors = Vec::new();
            let result = super::private_mode_acl(&root, dry_run, &mut errors, Some(&file));
            assert_eq!(result, ExitCode::from(1), "dry_run={dry_run}");
            let errors = String::from_utf8(errors).unwrap();
            assert_eq!(errors.lines().count(), 1, "dry_run={dry_run}");
            assert!(errors.contains("cannot inspect ACL"));
            assert!(errors.contains(&file.display().to_string()));
            assert!(errors.contains("injected ACL inspection denial"));
            assert_eq!(std::fs::read(&file).unwrap(), b"WAL marker");
            assert_eq!(std::fs::read(&db).unwrap(), b"database marker");
        }
        for dry_run in [true, false] {
            let mut errors = Vec::new();
            assert_eq!(
                super::private_mode_acl(&root, dry_run, &mut errors, None),
                ExitCode::SUCCESS,
                "a readable tree must still be accepted"
            );
            assert!(errors.is_empty());
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The refusals must fire, and must not fire on a usable root.
    ///
    /// Both arms matter. A guard that refuses everything is as useless as one that refuses
    /// nothing, and this tool repairs the production store -- an operator who cannot run it
    /// against a good root will reach for something less careful.
    ///
    /// The control-head arm is the load-bearing one: without a wal/ directory every row declines
    /// for a MISSING INPUT, which prints identically to declining for an underivable cursor. The
    /// output cannot distinguish them, so the refusal is the only thing that can.
    #[test]
    fn a_state_root_missing_its_inputs_is_refused_rather_than_reported_on() {
        let dir =
            broca_test_support::scratch_root().join(format!("sm-refusal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp root");

        // No run-index.db: refused for every operation, because there is nothing to repair.
        for operation in [
            "page-usage",
            "control-head",
            "reconcile-active",
            "private-mode",
            "wal-manifest",
        ] {
            let refusal = state_root_refusal(operation, &dir, false, true)
                .unwrap_or_else(|| panic!("{operation} must refuse a root with no run-index.db"));
            assert!(
                refusal.contains("run-index.db"),
                "the refusal must name what is missing, got: {refusal}"
            );
        }

        // Database present but no wal/: control-head refuses, the others proceed. This
        // asymmetry is the whole point -- only control-head reads WALs.
        for operation in ["control-head", "reconcile-active"] {
            assert!(
                state_root_refusal(operation, &dir, true, false)
                    .is_some_and(|text| text.contains("wal/")),
                "{operation} must refuse when its inputs are absent"
            );
        }
        assert!(
            state_root_refusal("page-usage", &dir, true, false).is_none(),
            "page-usage does not read WALs, so a missing wal/ must not block it"
        );
        assert!(
            state_root_refusal("wal-manifest", &dir, true, false).is_some(),
            "manifest rebuild must refuse when its WAL inputs are absent"
        );

        // A usable root refuses nothing.
        assert!(
            state_root_refusal("control-head", &dir, true, true).is_none(),
            "a complete state root must be accepted"
        );

        // A path that is not a directory at all is refused before anything else is consulted.
        assert!(
            state_root_refusal("page-usage", Path::new("/definitely/not/here"), true, true)
                .is_some(),
            "a non-directory root must be refused"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_head_folded_wal_is_not_permanently_missing() {
        let root = temp_root("control-head-archive");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wal-archive")).unwrap();
        let database = root.join("run-index.db");
        let store = broca_store::SqliteStore::open(&database).unwrap();
        let identity = BindIdentity::new("/project", "runner", "folded");
        let wal_store = FileWalStore::new(root.join("wal"));
        drop(wal_store.open(&identity).unwrap());
        let connection = rusqlite::Connection::open(&database).unwrap();
        for key in [
            "/project\u{1f}runner\u{1f}folded",
            "/project\u{1f}runner\u{1f}absent",
        ] {
            connection
                .execute(
                    "INSERT INTO meta (session, watermark_seq, context_epoch) VALUES (?1, 1, 0)",
                    [key],
                )
                .unwrap();
        }
        let before = store.preview_control_head_repair(&wal_store).unwrap();
        assert_eq!(before.scanned, 2);
        assert_eq!(before.declined, 2);
        assert_eq!(before.declined_no_wal, 1);
        let live = wal_store.wal_path(&identity);
        let container = root.join("wal-archive/fold-10.ark");
        let mut writer = broca_wal::archive::ArchiveWriter::create(&container)
            .unwrap()
            .without_feature_floor();
        writer
            .append(&session_addr(&identity), &std::fs::read(&live).unwrap())
            .unwrap();
        writer.seal().unwrap();
        std::fs::remove_file(live).unwrap();
        std::fs::remove_dir(root.join("wal")).unwrap();
        assert!(state_root_refusal("control-head", &root, true, false).is_none());
        let after = store.preview_control_head_repair(&wal_store).unwrap();
        assert_eq!(after.scanned, 2);
        assert_eq!(after.declined, 2);
        assert_eq!(after.declined_no_wal, 1, "folded is not absent");
        let mut damaged = std::fs::read(&container).unwrap();
        damaged[64] ^= 1;
        std::fs::write(&container, damaged).unwrap();
        let corrupt = store.preview_control_head_repair(&wal_store).unwrap();
        assert_eq!(corrupt.scanned, 2);
        assert_eq!(corrupt.declined_no_wal, 1, "unreadable is not absent");
        drop(connection);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn index_session(store: &broca_store::SqliteStore, run_id: &str, session: BindIdentity) {
        store
            .index_run_active(&RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: run_id.to_owned(),
                session,
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("index session");
    }

    fn independent_session_addr(identity: &BindIdentity) -> String {
        let bytes = format!(
            "{}\u{1f}{}\u{1f}{}",
            identity.project_root.display(),
            identity.harness,
            identity.session
        );
        let digest = bytes
            .as_bytes()
            .iter()
            .fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        format!("{digest:016x}")
    }

    #[test]
    fn creating_a_wal_appends_exactly_one_manifest_mapping() {
        let state_root = temp_root("manifest-created-once");
        let _ = std::fs::remove_dir_all(&state_root);
        let identity = BindIdentity::new("/tmp/p", "runner", "s1");
        let store = FileWalStore::new(state_root.join("wal"));

        drop(store.open(&identity).expect("create WAL"));
        drop(store.open(&identity).expect("reopen WAL"));

        let manifest = std::fs::read_to_string(store.manifest_path()).expect("read manifest");
        let lines: Vec<_> = manifest.lines().collect();
        assert_eq!(lines.len(), 1, "a reopen must not append a second mapping");
        let expected_addr = independent_session_addr(&identity);
        assert_eq!(expected_addr, session_addr(&identity));
        let mut fields = lines[0].splitn(3, "  ");
        assert_eq!(fields.next(), Some(expected_addr.as_str()));
        let lineage = fields.next().expect("lineage field");
        assert!(!lineage.is_empty(), "the minted lineage must be recorded");
        assert_eq!(
            fields.next(),
            Some("/tmp/p\u{1f}runner\u{1f}s1"),
            "the diagnostic value is the full bind triple"
        );
        let header_lineage = broca_wal::read_records_range(
            store.wal_path(&identity),
            DEFAULT_MAX_READ_BYTES,
            0,
            None,
            None,
        )
        .expect("read WAL header")
        .lineage_id
        .expect("lineage frame");
        assert_eq!(lineage, header_lineage.to_string());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(store.manifest_path())
                    .expect("manifest metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(&state_root)
                    .expect("manifest directory metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn a_manifest_failure_cannot_fail_wal_creation_or_use() {
        let state_root = temp_root("manifest-failure-is-nonfatal");
        let _ = std::fs::remove_dir_all(&state_root);
        std::fs::create_dir_all(state_root.join(WAL_MANIFEST_FILENAME))
            .expect("manifest path occupied by directory");
        let identity = BindIdentity::new("/tmp/p", "runner", "manifest-failure");
        let store = FileWalStore::new(state_root.join("wal"));

        let mut wal = store
            .open(&identity)
            .expect("manifest failure is non-fatal");
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
            .expect("WAL append remains usable");
        assert_eq!(wal.replay().expect("WAL replay remains usable").len(), 1);

        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn concurrent_manifest_appends_are_complete_and_unspliced() {
        const WRITERS: usize = 48;
        let state_root = temp_root("manifest-concurrent");
        let _ = std::fs::remove_dir_all(&state_root);
        let store = std::sync::Arc::new(FileWalStore::new(state_root.join("wal")));
        let start = std::sync::Arc::new(std::sync::Barrier::new(WRITERS));
        let identities: Vec<_> = (0..WRITERS)
            .map(|index| {
                BindIdentity::new(
                    "/tmp/concurrent-project",
                    "runner",
                    format!("writer-{index:03}-{}", "x".repeat(8192)),
                )
            })
            .collect();
        let threads: Vec<_> = identities
            .iter()
            .cloned()
            .map(|identity| {
                let store = std::sync::Arc::clone(&store);
                let start = std::sync::Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    drop(store.open(&identity).expect("create concurrent WAL"));
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("manifest writer thread");
        }

        let manifest = std::fs::read_to_string(store.manifest_path()).expect("read manifest");
        let lines: Vec<_> = manifest.lines().collect();
        assert_eq!(lines.len(), WRITERS, "one complete line per created WAL");
        let expected: std::collections::HashSet<_> = identities
            .iter()
            .map(|identity| {
                (
                    independent_session_addr(identity),
                    format!(
                        "{}\u{1f}{}\u{1f}{}",
                        identity.project_root.display(),
                        identity.harness,
                        identity.session
                    ),
                )
            })
            .collect();
        let parsed: std::collections::HashSet<_> = lines
            .iter()
            .map(|line| {
                let mut fields = line.splitn(3, "  ");
                let addr = fields.next().expect("address").to_owned();
                let lineage = fields.next().expect("lineage");
                assert_eq!(lineage.len(), 36, "lineage field must remain whole");
                let identity = fields.next().expect("identity").to_owned();
                (addr, identity)
            })
            .collect();
        assert_eq!(
            parsed, expected,
            "no fields from concurrent lines may splice"
        );

        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn wal_manifest_rebuild_reproduces_known_store_to_header_mappings() {
        let state_root = temp_root("wal-manifest-rebuild");
        let _ = std::fs::remove_dir_all(&state_root);
        std::fs::create_dir_all(&state_root).expect("state root");
        let database = state_root.join("run-index.db");
        let store = broca_store::SqliteStore::open(&database).expect("store");
        let indexed = BindIdentity::new("/tmp/indexed-project", "runner", "indexed-session");
        let projected = BindIdentity::new("/tmp/projected-project", "runner", "meta-session");
        index_session(&store, "run-indexed", indexed.clone());
        rusqlite::Connection::open(&database)
            .expect("second store connection")
            .execute(
                "INSERT INTO meta (
                    session, watermark_seq, context_epoch, completed_steps, usage_json
                 ) VALUES (?1, 0, 0, 0, '{}')",
                [format!(
                    "{}\u{1f}{}\u{1f}{}",
                    projected.project_root.display(),
                    projected.harness,
                    projected.session
                )],
            )
            .expect("insert meta-only session");

        let wal_store = FileWalStore::new(state_root.join("wal"));
        drop(wal_store.open(&indexed).expect("indexed WAL"));
        drop(wal_store.open(&projected).expect("meta WAL"));
        let manifest_path = wal_store.manifest_path();
        let original: std::collections::BTreeSet<_> = std::fs::read_to_string(&manifest_path)
            .expect("creation manifest")
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(original.len(), 2);
        std::fs::remove_file(&manifest_path).expect("simulate lost manifest");

        // Inject known-stopped liveness rather than depend on the host's daemon.
        assert_eq!(
            wal_manifest_with(&state_root, false, WriterLiveness::stopped()),
            std::process::ExitCode::SUCCESS,
            "report-only mode can derive the replacement"
        );
        assert!(
            !manifest_path.exists(),
            "report-only mode must not recreate the manifest"
        );
        assert_eq!(
            wal_manifest_with(&state_root, true, WriterLiveness::stopped()),
            std::process::ExitCode::SUCCESS,
            "explicit write rebuilds the manifest"
        );
        let rebuilt: std::collections::BTreeSet<_> = std::fs::read_to_string(&manifest_path)
            .expect("rebuilt manifest")
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(rebuilt, original, "rebuild must preserve every mapping");

        // THE REFUSAL, AND ITS SCOPE. A rebuild beside a live daemon renames a replacement over a
        // file the daemon appends to, so a session created during the rebuild has its mapping
        // written and then discarded -- while this command printed `written N` and exited 0.
        //
        // The second assertion is the one that keeps the guard from being a blunt instrument:
        // report-only must STILL WORK with a daemon up, because reading coverage is exactly what
        // an operator does while the system is serving. A guard that refused both modes would be
        // green here and useless in practice.
        // Asserts the manifest is BYTE-UNCHANGED rather than absent. My first version deleted it
        // first, which broke the permissions check below AND tested the weaker property: "a
        // refusal creates nothing" does not rule out a refusal that truncates or partially writes
        // an EXISTING manifest, which is the state an operator actually has.
        let before = std::fs::read(&manifest_path).expect("manifest before the refusal");
        assert_eq!(
            wal_manifest_with(
                &state_root,
                true,
                WriterLiveness {
                    state: "yes".into(),
                    reason: "pid 4242".into()
                }
            ),
            std::process::ExitCode::from(3),
            "a live daemon must refuse the write rather than silently losing an append"
        );
        assert_eq!(
            std::fs::read(&manifest_path).expect("manifest after the refusal"),
            before,
            "a refused rebuild must leave the manifest byte-identical"
        );
        assert_eq!(
            wal_manifest_with(
                &state_root,
                false,
                WriterLiveness {
                    state: "yes".into(),
                    reason: "pid 4242".into()
                }
            ),
            std::process::ExitCode::SUCCESS,
            "report-only must still work with a live daemon; it writes nothing"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&manifest_path)
                    .expect("manifest metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn unreadable_store_refuses_without_emitting_a_partial_manifest() {
        let state_root = temp_root("wal-manifest-unreadable-store");
        let _ = std::fs::remove_dir_all(&state_root);
        std::fs::create_dir_all(state_root.join("wal")).expect("WAL directory");
        std::fs::write(state_root.join("run-index.db"), b"not a sqlite database")
            .expect("corrupt store fixture");
        let manifest_path = state_root.join("wal-manifest");
        std::fs::write(&manifest_path, b"existing manifest must survive\n")
            .expect("existing manifest");

        assert_eq!(
            // Known-stopped probes reach the separate input-error path.
            wal_manifest_with(&state_root, true, WriterLiveness::stopped()),
            std::process::ExitCode::from(2),
            "a store that cannot be read must refuse"
        );
        assert_eq!(
            std::fs::read(&manifest_path).expect("manifest remains readable"),
            b"existing manifest must survive\n",
            "refusal must not truncate, replace, or append the manifest"
        );

        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn wal_manifest_unknown_refuses_before_sqlite_initialisation() {
        assert_manifest_refuses_before_open(WriterLiveness::unknown(
            "enumerator unavailable".into(),
        ));
    }

    #[test]
    fn wal_manifest_live_refuses_before_sqlite_initialisation() {
        assert_manifest_refuses_before_open(WriterLiveness {
            state: "yes".into(),
            reason: "idle daemon pid 4242".into(),
        });
    }

    fn assert_manifest_refuses_before_open(liveness: WriterLiveness) {
        let root = temp_root(&format!("manifest-{}-before-open", liveness.state));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let database = root.join("run-index.db");
        let manifest = root.join("wal-manifest");
        std::fs::write(&database, []).unwrap();
        std::fs::write(&manifest, b"mapping must survive\n").unwrap();
        let wal = root.join("wal").join("untouched.wal");
        std::fs::write(&wal, b"WAL bytes must survive").unwrap();
        assert_eq!(
            wal_manifest_with(&root, true, liveness),
            ExitCode::from(3),
            "liveness refusal must not become an input error or permission to open SQLite"
        );
        assert_eq!(std::fs::read(&database).unwrap(), b"");
        assert_eq!(std::fs::read(&manifest).unwrap(), b"mapping must survive\n");
        assert_eq!(std::fs::read(&wal).unwrap(), b"WAL bytes must survive");
        assert!(!root.join("run-index.db-wal").exists());
        assert!(!root.join("run-index.db-shm").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_manifest_replacement_temporary_is_private() {
        let fixture = BroadParent::new("manifest-replacement");
        let path = fixture.path.join("wal-manifest");
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || {
                replace_manifest(&path, "new manifest").unwrap();
                replace_manifest(&path, "replacement manifest").unwrap();
            },
        );
        assert_eq!(
            seen.get(),
            2,
            "temporary files must be checked before cleanup"
        );
        assert_private(&path);
    }
}
