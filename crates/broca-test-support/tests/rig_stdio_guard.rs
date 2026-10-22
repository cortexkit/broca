use std::path::{Path, PathBuf};

fn code_mask(source: &str) -> Vec<bool> {
    let bytes = source.as_bytes();
    let mut mask = vec![true; bytes.len()];
    let (mut index, mut block_depth) = (0, 0usize);
    let mut in_string = false;
    while index < bytes.len() {
        if block_depth > 0 {
            mask[index] = false;
            if bytes.get(index..index + 2) == Some(b"/*") {
                if index + 1 < bytes.len() {
                    mask[index + 1] = false;
                }
                block_depth += 1;
                index += 2;
            } else if bytes.get(index..index + 2) == Some(b"*/") {
                if index + 1 < bytes.len() {
                    mask[index + 1] = false;
                }
                block_depth -= 1;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if in_string {
            mask[index] = false;
            if bytes[index] == b'\\' {
                if index + 1 < bytes.len() {
                    mask[index + 1] = false;
                }
                index += 2;
            } else if bytes[index] == b'"' {
                in_string = false;
                index += 1;
            } else {
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"//") {
            while index < bytes.len() && bytes[index] != b'\n' {
                mask[index] = false;
                index += 1;
            }
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            mask[index] = false;
            if index + 1 < bytes.len() {
                mask[index + 1] = false;
            }
            block_depth = 1;
            index += 2;
        } else if bytes[index] == b'"' {
            mask[index] = false;
            in_string = true;
            index += 1;
        } else {
            index += 1;
        }
    }
    mask
}

fn rust_files(directory: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn unwrapped_rig_spawns(source: &str) -> Vec<String> {
    let mask = code_mask(source);
    let mut violations = Vec::new();
    for (position, _) in source.match_indices(".spawn(") {
        if !mask.get(position).copied().unwrap_or(false) {
            continue;
        }
        let statement_start = source[..position].rfind(';').map_or(0, |index| index + 1);
        let mut expression = &source[statement_start..position];
        let receiver = source[..position].trim_end();
        if receiver
            .chars()
            .last()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            let variable = receiver
                .rsplit(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .next()
                .unwrap_or_default();
            if let Some(start) = command_definition(source, &mask, variable, position) {
                expression = &source[start..position];
            }
        }
        if expression.contains("rig_command(") || expression.contains("tokio_rig_command(") {
            continue;
        }
        let is_rig_binary = [
            "ck-subc",
            "fake-aft-stub",
            "ck-broca",
            "subc_core",
            "daemon_bin",
            "module_bin",
            "ckdev_command(",
            "--fixtures",
        ]
        .iter()
        .any(|marker| expression.contains(marker));
        if !is_rig_binary {
            continue;
        }
        // Version reads are bounded probes whose output must remain available to the test.
        if expression.contains("--version") && expression.contains("Stdio::piped()") {
            continue;
        }
        violations.push(format!(
            "line {}: {}",
            source[..position].lines().count(),
            expression.trim()
        ));
    }
    violations
}

fn command_definition(source: &str, mask: &[bool], variable: &str, before: usize) -> Option<usize> {
    let declarations = [format!("let {variable} ="), format!("let mut {variable} =")];
    declarations
        .iter()
        .flat_map(|declaration| source[..before].match_indices(declaration))
        .filter_map(|(position, _)| {
            mask.get(position)
                .copied()
                .unwrap_or(false)
                .then_some(position)
        })
        .max()
}

/// The part of a `src/` file that holds tests: the whole of a dedicated test file, otherwise the
/// text from its first code-level `#[cfg(test)]` on. Production code before that marker may
/// legitimately start the same binaries, so it is not scanned.
fn inline_test_source(path: &Path, source: &str) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let dedicated = name == "tests.rs"
        || name.ends_with("_tests.rs")
        || path.components().any(|part| part.as_os_str() == "tests");
    if dedicated {
        return Some(source.to_owned());
    }
    let mask = code_mask(source);
    source
        .match_indices("#[cfg(test)]")
        .find(|(position, _)| mask.get(*position).copied().unwrap_or(false))
        // Blank lines stand in for the production part, so reported line numbers stay true.
        .map(|(position, _)| {
            let skipped = source[..position].matches('\n').count();
            "\n".repeat(skipped) + &source[position..]
        })
}

#[test]
fn rig_children_use_owned_stdio_logs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut sources = Vec::new();
    for crate_name in [
        "broca-subc",
        "broca-module-serve",
        "broca-module",
        "ck-import",
        "broca-provider",
    ] {
        let crate_dir = root.join("crates").join(crate_name);
        let mut files = Vec::new();
        rust_files(&crate_dir.join("tests"), &mut files);
        for path in files {
            if let Ok(source) = std::fs::read_to_string(&path) {
                sources.push((path, source));
            }
        }
        // Inline lib tests can start a daemon too, and a scan of `tests/` alone never saw them.
        let mut files = Vec::new();
        rust_files(&crate_dir.join("src"), &mut files);
        for path in files {
            if let Some(source) = std::fs::read_to_string(&path)
                .ok()
                .and_then(|source| inline_test_source(&path, &source))
            {
                sources.push((path, source));
            }
        }
    }

    // A scan that read nothing, or lost the inline sources, would pass on no evidence.
    assert!(
        sources.len() > 100,
        "rig stdio guard scanned only {} test sources",
        sources.len()
    );
    let inline_daemon_test = root.join("crates/broca-module-serve/src/role_plane_tests.rs");
    assert!(
        sources.iter().any(|(path, _)| path == &inline_daemon_test),
        "rig stdio guard no longer scans inline test sources such as {}",
        inline_daemon_test.display()
    );

    let violations: Vec<_> = sources
        .iter()
        .filter_map(|(path, source)| {
            let sites = unwrapped_rig_spawns(source);
            (!sites.is_empty()).then(|| {
                format!(
                    "{}:\n{}",
                    path.strip_prefix(&root).unwrap_or(path).display(),
                    sites.join("\n")
                )
            })
        })
        .collect();

    assert!(
        violations.is_empty(),
        "long-lived rig children must use broca_test_support::rig_command or tokio_rig_command:\n{}",
        violations.join("\n")
    );
}

#[cfg(test)]
mod scanner_tests {
    use super::unwrapped_rig_spawns;

    #[test]
    fn detects_a_daemon_spawn_without_the_shared_helper() {
        // The constructor name is assembled at compile time so this fixture's own source text
        // holds no spawn: the state-root isolation guard scans test files as text and would
        // otherwise report this string as an unisolated daemon spawn.
        let source = r#"
            fn start() {
                let child = tokio::process::SPAWN(&daemon_bin)
                    .kill_on_drop(true)
                    .spawn();
            }
        "#
        .replace("SPAWN", concat!("Command", "::new"));
        assert_eq!(unwrapped_rig_spawns(&source).len(), 1);
    }

    #[test]
    fn accepts_a_daemon_spawn_through_the_shared_helper() {
        // Assembled at compile time for the same reason as the fixture above.
        let source = r#"
            fn start() {
                let child = SPAWN(&daemon_bin, &dir, "daemon")
                    .unwrap()
                    .kill_on_drop(true)
                    .spawn();
            }
        "#
        .replace("SPAWN", concat!("tokio_rig_", "command"));
        assert!(unwrapped_rig_spawns(&source).is_empty());
    }
}
