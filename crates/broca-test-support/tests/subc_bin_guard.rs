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
        let end = start + name.len();
        from = end;
        if !mask[start..end].iter().all(|enabled| *enabled)
            || start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_')
            || end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
        {
            continue;
        }
        let Some(open) =
            (end..bytes.len()).find(|index| mask[*index] && !bytes[*index].is_ascii_whitespace())
        else {
            continue;
        };
        if bytes[open] == b'(' {
            if let Some(close) = matching_paren(source, mask, open) {
                found.push((start, &source[open + 1..close]));
            }
        }
    }
    found
}

fn rust_files(directory: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("reading crates source tree") {
        let path = entry.expect("reading crates source entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn direct_subc_env_reads(source: &str) -> Vec<(usize, &'static str, &'static str)> {
    let mask = code_mask(source);
    let mut reads = Vec::new();
    for method in ["var_os", "var"] {
        for (start, arguments) in calls(source, &mask, method) {
            let argument = arguments.trim_start();
            for key in ["SUBC_CORE_BIN", "FAKE_AFT_STUB_BIN"] {
                if argument.starts_with(&format!("\"{key}\"")) {
                    let line = source[..start]
                        .bytes()
                        .filter(|byte| *byte == b'\n')
                        .count()
                        + 1;
                    reads.push((line, method, key));
                }
            }
        }
    }
    reads
}

#[test]
fn real_daemon_binary_environment_reads_use_shared_resolver() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let resolver_path = workspace.join("crates/broca-test-support/src/lib.rs");
    let resolver = std::fs::read_to_string(&resolver_path).expect("reading shared resolver");
    assert!(
        resolver.contains("pub fn subc_bin("),
        "shared resolver is missing"
    );
    let resolver_mask = code_mask(&resolver);
    assert!(
        calls(&resolver, &resolver_mask, "var_os")
            .iter()
            .any(|(_, arguments)| arguments.trim() == "env_key"),
        "positive control: the shared resolver must read its caller's environment key"
    );

    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let mut scanned = 0usize;
    let mut offenders = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("reading {} failed: {error}", file.display()));
        scanned += 1;
        if file == resolver_path {
            continue;
        }
        let relative = file.strip_prefix(&workspace).unwrap_or(&file);
        offenders.extend(
            direct_subc_env_reads(&source)
                .into_iter()
                .map(|(line, method, key)| {
                    format!("{}:{line}: {method}(\"{key}\")", relative.display())
                }),
        );
    }
    assert!(scanned > 0, "the scan must cover Rust files under crates/");
    assert!(
        offenders.is_empty(),
        "real-daemon binary environment reads outside subc_bin:\n{}",
        offenders.join("\n")
    );
}
