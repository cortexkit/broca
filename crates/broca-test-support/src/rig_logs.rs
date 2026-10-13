//! Log evidence collected before a failing real-daemon rig is torn down.

use std::collections::VecDeque;
use std::io::BufRead;
use std::path::Path;

/// Log tails, watchdog stall files and the WAL file inventory of a real-daemon rig.
/// `data_home` is the `cortexkit` directory under the daemon's `XDG_DATA_HOME`;
/// `state_root` is broca's `BROCA_STATE_ROOT`, which may live elsewhere.
pub fn rig_log_diagnostics(data_home: &Path, state_root: &Path) -> String {
    let mut output = String::new();
    let run_logs = data_home.join("run/logs");
    append_file_tail(
        &mut output,
        "supervised broca stderr",
        &run_logs.join("broca.stderr.log"),
    );
    append_file_tail(
        &mut output,
        "supervised aft stderr",
        &run_logs.join("aft.stderr.log"),
    );
    append_matching_file_tails(
        &mut output,
        "daemon dated subc.*.log segments",
        &run_logs,
        |name| name.starts_with("subc.") && name.ends_with(".log"),
    );
    append_matching_file_tails(
        &mut output,
        "broca structured log segments",
        &data_home.join("broca/logs"),
        |_| true,
    );
    append_matching_file_tails(
        &mut output,
        "broca watchdog stall-*.jsonl files",
        &state_root.join("logs"),
        |name| name.starts_with("stall-") && name.ends_with(".jsonl"),
    );
    append_wal_inventory(&mut output, &state_root.join("wal"));
    output
}

pub fn append_file_tail(output: &mut String, label: &str, path: &Path) {
    output.push_str(&format!("--- {label}: {} ---\n", path.display()));
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            output.push_str("absent\n");
            return;
        }
        Err(error) => {
            output.push_str(&format!("unreadable: {error}\n"));
            return;
        }
    };
    let mut tail = VecDeque::with_capacity(200);
    for line in std::io::BufReader::new(file).lines() {
        match line {
            Ok(mut line) => {
                const MAX_LINE_BYTES: usize = 1024;
                if line.len() > MAX_LINE_BYTES {
                    let mut end = MAX_LINE_BYTES;
                    while !line.is_char_boundary(end) {
                        end -= 1;
                    }
                    line.truncate(end);
                    line.push_str(" …[truncated]");
                }
                if tail.len() == 200 {
                    tail.pop_front();
                }
                tail.push_back(line);
            }
            Err(error) => {
                output.push_str(&format!("read error: {error}\n"));
                break;
            }
        }
    }
    if tail.is_empty() {
        output.push_str("(empty)\n");
    } else {
        for line in tail {
            output.push_str(&line);
            output.push('\n');
        }
    }
}

fn append_matching_file_tails(
    output: &mut String,
    label: &str,
    directory: &Path,
    matches: impl Fn(&str) -> bool,
) {
    let mut paths = match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(&matches)
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            output.push_str(&format!(
                "--- {label}: {} ---\n{}\n",
                directory.display(),
                if error.kind() == std::io::ErrorKind::NotFound {
                    "absent".to_owned()
                } else {
                    format!("unreadable: {error}")
                }
            ));
            return;
        }
    };
    paths.sort();
    if paths.is_empty() {
        output.push_str(&format!(
            "--- {label}: {} ---\nabsent (no matching files)\n",
            directory.display()
        ));
    }
    for path in paths {
        append_file_tail(output, label, &path);
    }
}

fn append_wal_inventory(output: &mut String, wal_dir: &Path) {
    output.push_str(&format!(
        "--- WAL file inventory: {} ---\n",
        wal_dir.display()
    ));
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(error) => {
            output.push_str(&format!("unavailable: {error}\n"));
            return;
        }
    };
    let mut files = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter_map(|path| {
            let size = std::fs::metadata(&path).ok()?.len();
            Some((path, size))
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.is_empty() {
        output.push_str("(no WAL files)\n");
    }
    for (path, size) in files {
        output.push_str(&format!("{}: {size} bytes\n", path.display()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rig_logs_include_module_stderr_and_daemon_exit_and_spawn_errors() {
        let root = crate::scratch_root().join("rig-log-evidence");
        let data_home = root.join("data-home/cortexkit");
        let state_root = root.join("rig-state-root");
        let logs = data_home.join("run/logs");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(logs.join("broca.stderr.log"), "broca startup stderr\n").unwrap();
        std::fs::write(
            logs.join("subc.2026-10-13.log"),
            "module broca exited status=1\nspawn broca failed: Text file busy (os error 26)\n",
        )
        .unwrap();
        std::fs::write(logs.join("unrelated.log"), "not daemon evidence").unwrap();
        let output = rig_log_diagnostics(&data_home, &state_root);
        assert!(output.contains("broca startup stderr"), "{output}");
        assert!(output.contains("module broca exited status=1"), "{output}");
        assert!(
            output.contains("spawn broca failed: Text file busy (os error 26)"),
            "{output}"
        );
        assert!(!output.contains("not daemon evidence"), "{output}");
    }
}
