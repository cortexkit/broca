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

#[test]
fn rig_children_use_owned_stdio_logs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for crate_name in [
        "broca-subc",
        "broca-module-serve",
        "broca-module",
        "ck-import",
        "broca-provider",
    ] {
        rust_files(
            &root.join("crates").join(crate_name).join("tests"),
            &mut files,
        );
    }

    let violations: Vec<_> = files
        .into_iter()
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path).ok()?;
            let sites = unwrapped_rig_spawns(&source);
            (!sites.is_empty()).then(|| {
                format!(
                    "{}:\n{}",
                    path.strip_prefix(&root).unwrap_or(&path).display(),
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
        let source = r#"
            fn start() {
                let child = tokio::process::Command::new(&daemon_bin)
                    .kill_on_drop(true)
                    .spawn();
            }
        "#;
        assert_eq!(unwrapped_rig_spawns(source).len(), 1);
    }

    #[test]
    fn accepts_a_daemon_spawn_through_the_shared_helper() {
        let source = r#"
            fn start() {
                let child = tokio_rig_command(&daemon_bin, &dir, "daemon")
                    .unwrap()
                    .kill_on_drop(true)
                    .spawn();
            }
        "#;
        assert!(unwrapped_rig_spawns(source).is_empty());
    }
}
