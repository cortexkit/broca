use std::path::{Path, PathBuf};

/// Mark Rust code while ignoring comments and string contents. Source literals are examined as
/// call arguments below, but source-shaped examples in comments and test fixtures are not calls.
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

fn matching_paren(source: &str, mask: &[bool], open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    for index in open..bytes.len() {
        if !mask[index] {
            continue;
        }
        match bytes[index] {
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn calls<'a>(source: &'a str, mask: &[bool], name: &str) -> Vec<(usize, &'a str)> {
    let bytes = source.as_bytes();
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(relative) = source[from..].find(name) {
        let start = from + relative;
        let open = start + name.len();
        from = open;
        if !mask[start..open].iter().all(|enabled| *enabled) {
            continue;
        }
        let Some(open) =
            (open..bytes.len()).find(|index| mask[*index] && !bytes[*index].is_ascii_whitespace())
        else {
            continue;
        };
        if bytes[open] != b'(' {
            continue;
        }
        if let Some(close) = matching_paren(source, mask, open) {
            found.push((start, &source[open + 1..close]));
        }
    }
    found
}

fn unwrapped_ck_spawn_sites(source: &str) -> Vec<String> {
    let mask = code_mask(source);
    let mut violations = Vec::new();
    for (start, argument) in calls(source, &mask, "Command::new") {
        if (argument.contains("CARGO_BIN_EXE_ck-") || argument.contains("\"ck-"))
            && !argument.contains("ckdev_binary(")
            && !argument.contains("ckdev_command(")
        {
            violations.push(format!("Command::new at byte {start}: {argument}"));
        }
    }

    let helper_spans: Vec<_> = ["ckdev_binary", "ckdev_command"]
        .into_iter()
        .flat_map(|name| {
            calls(source, &mask, name)
                .into_iter()
                .map(|(start, argument)| (start, start + name.len() + 2 + argument.len()))
        })
        .collect();
    for (start, argument) in calls(source, &mask, "env!") {
        if !argument.contains("CARGO_BIN_EXE_ck-") {
            continue;
        }
        let marker = start + "env!(".len() + argument.find("CARGO_BIN_EXE_ck-").unwrap();
        if !helper_spans
            .iter()
            .any(|(begin, end)| *begin < marker && marker < *end)
        {
            violations.push(format!(
                "unwrapped ck binary env! at byte {start}: {argument}"
            ));
        }
    }
    violations
}

fn rust_files(directory: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn is_rig_source(path: &Path, source: &str) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path
            .components()
            .any(|component| component.as_os_str() == "broca-test-support")
        || source.contains("#[cfg(test)]")
}

#[test]
fn test_code_never_spawns_ck_binaries_under_production_names() {
    let cargo_var = ["CARGO_BIN_EXE_", "ck-", "broca"].concat();
    let executable_name = ["ck-", "broca"].concat();
    let direct = format!(r#"Command::new(env!("{cargo_var}")).spawn();"#);
    let violations = unwrapped_ck_spawn_sites(&direct);
    assert_eq!(
        violations.len(),
        2,
        "positive control was not caught: {violations:?}"
    );
    assert!(violations[0].contains("Command::new"), "{violations:?}");
    assert!(violations[1].contains("env!"), "{violations:?}");
    let dev_linked = format!(
        r#"Command::new(broca_test_support::ckdev_binary(env!("{cargo_var}"), scratch_root())).spawn(); // {executable_name}"#
    );
    assert!(
        unwrapped_ck_spawn_sites(&dev_linked).is_empty(),
        "the approved helper path should pass: {:?}",
        unwrapped_ck_spawn_sites(&dev_linked)
    );

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let mut scanned = 0;
    let mut offenders = Vec::new();
    for file in files {
        if file.ends_with("crates/broca-test-support/tests/ckdev_guard.rs") {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        if !is_rig_source(&file, &source) {
            continue;
        }
        scanned += 1;
        let relative = file.strip_prefix(&workspace).unwrap_or(&file);
        offenders.extend(
            unwrapped_ck_spawn_sites(&source)
                .into_iter()
                .map(|site| format!("{}: {site}", relative.display())),
        );
    }
    assert!(scanned > 0, "the scan must cover test and rig source files");
    assert!(
        offenders.is_empty(),
        "production-named ck spawn sites:\n{}",
        offenders.join("\n")
    );
}
