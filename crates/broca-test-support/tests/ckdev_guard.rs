use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

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
        if bytes[index] == b'r' {
            let mut quote = index + 1;
            while bytes.get(quote) == Some(&b'#') {
                quote += 1;
            }
            if bytes.get(quote) == Some(&b'"') {
                let terminator = format!("\"{}", "#".repeat(quote - index - 1));
                let end = source[quote + 1..]
                    .find(&terminator)
                    .map_or(bytes.len(), |relative| {
                        quote + 1 + relative + terminator.len()
                    });
                mask[index..end].fill(false);
                index = end;
                continue;
            }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuildPath {
    CurrentExe,
    Target,
}

fn build_path(expression: &str, bindings: &BTreeMap<String, BuildPath>) -> Option<BuildPath> {
    let mut expression = expression.to_string();
    // ckdev helpers copy executables outside the build directory, avoiding
    // macOS exec restrictions on removable build volumes. Their input paths
    // are not the paths the rig subsequently executes.
    let mask = code_mask(&expression);
    let mut helpers: Vec<_> = [
        "ckdev_binary",
        "ckdev_binary_pair",
        "ckdev_binary_with_companion",
        "ckdev_command",
    ]
    .into_iter()
    .flat_map(|name| {
        calls(&expression, &mask, name)
            .into_iter()
            .map(move |(start, argument)| (start, start + name.len() + argument.len() + 2))
    })
    .collect();
    helpers.sort_unstable();
    for (start, end) in helpers.into_iter().rev() {
        expression.replace_range(start..end, &" ".repeat(end - start));
    }
    let mask = code_mask(&expression);
    let compact: String = expression.split_whitespace().collect();
    if expression.contains("target/")
        || expression.contains("target\\\\")
        || compact.contains(".join(\"target\")")
        || compact.contains("::from(\"target\")")
        || compact.contains("::new(\"target\")")
        || compact == "\"target\""
    {
        return Some(BuildPath::Target);
    }
    if !calls(&expression, &mask, "current_exe").is_empty() {
        return Some(BuildPath::CurrentExe);
    }
    for (name, path) in bindings {
        if expression.contains(&format!("{{{name}}}")) || expression.contains(&format!("{{{name}:"))
        {
            return Some(*path);
        }
    }
    let mut start = 0;
    let bytes = expression.as_bytes();
    while start < bytes.len() {
        if mask[start] && (bytes[start].is_ascii_alphabetic() || bytes[start] == b'_') {
            let mut end = start + 1;
            while end < bytes.len()
                && mask[end]
                && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
            {
                end += 1;
            }
            if let Some(path) = bindings.get(&expression[start..end]) {
                return Some(*path);
            }
            start = end;
        } else {
            start += 1;
        }
    }
    None
}

fn indirect_build_path_sites(source: &str) -> Vec<String> {
    let mask = code_mask(source);
    let mut bindings = BTreeMap::new();
    let mut events = Vec::new();
    for (start, _) in source.match_indices("fn ") {
        if mask[start] {
            events.push((start, true, "", ""));
        }
    }
    // Follow simple let aliases such as current_exe -> shell-quoted path ->
    // script. This source fence does not evaluate Rust: reassignment or paths
    // returned by other functions need additional fixtures and tracking.
    for (start, _) in source.match_indices("let ") {
        if !mask[start] {
            continue;
        }
        let Some(end) =
            (start..source.len()).find(|index| mask[*index] && source.as_bytes()[*index] == b';')
        else {
            continue;
        };
        let statement = &source[start + 4..end];
        let Some((declaration, value)) = statement.split_once('=') else {
            continue;
        };
        let name = declaration
            .trim()
            .strip_prefix("mut ")
            .unwrap_or(declaration.trim());
        let name = name.split(':').next().unwrap().trim();
        if !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            events.push((end, true, name, value));
        }
    }
    for name in [
        "fs::write",
        "write!",
        "writeln!",
        "write_all",
        "json!",
        ".arg",
        ".args",
        ".env",
        "Command::new",
        "rig_command",
        "tokio_rig_command",
    ] {
        for (start, arguments) in calls(source, &mask, name) {
            events.push((start, false, name, arguments));
        }
    }
    events.sort_by_key(|event| event.0);
    let mut violations = Vec::new();
    for (start, binding, name, value) in events {
        if binding && name.is_empty() {
            bindings.clear();
            continue;
        }
        // Writing fixture files under target is valid. Inspect the written
        // content for executable paths, not the output filename.
        let value = if ["fs::write", "write!", "writeln!"].contains(&name) {
            after_first_argument(value)
        } else {
            value
        };
        let path = build_path(value, &bindings);
        if binding {
            bindings.remove(name);
            if let Some(path) = path {
                bindings.insert(name.to_string(), path);
            }
        } else if let Some(path) = path {
            // The self-reexec exception applies only to the command's program.
            // Scripts, configs and child arguments must use a published copy
            // so a supervising process never executes the build-tree path.
            let direct = ["Command::new", "rig_command", "tokio_rig_command"].contains(&name);
            if !direct || path == BuildPath::Target {
                violations.push(format!("{name} at byte {start} receives {path:?}: {value}"));
            }
        }
    }
    violations
}

fn after_first_argument(arguments: &str) -> &str {
    let mask = code_mask(arguments);
    let mut depth = 0;
    for (index, byte) in arguments.bytes().enumerate() {
        if !mask[index] {
            continue;
        }
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => return &arguments[index + 1..],
            _ => {}
        }
    }
    ""
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

#[test]
fn indirect_launch_checker_tracks_build_paths_but_allows_direct_self_reexec() {
    for source in [
        r##"let executable = std::env::current_exe().unwrap();
            let quoted = executable.to_string_lossy().replace('\'', "'\\''");
            std::fs::write(&script, format!("#!/bin/sh\nexec '{quoted}'\n"));"##,
        r#"json!({"modules":{"fake":{"program":std::env::current_exe().unwrap()}}});"#,
        r#"let executable = root.join("target/debug/fake");
            json!({"program":executable});"#,
        r##"std::fs::write(script, "#!/bin/sh\nexec target/release/fake\n");"##,
        r#"Command::new("target/debug/fake").spawn();"#,
        r#"let executable = root.join("target").join("debug").join("fake");
            json!({"program":executable});"#,
        r#"Command::new("target/custom-profile/fake").spawn();"#,
        r#"let executable = std::env::current_exe().unwrap();
            Command::new("sh").arg(executable).spawn();"#,
    ] {
        assert!(
            !indirect_build_path_sites(source).is_empty(),
            "missed indirect build path: {source}"
        );
    }
    for source in [
        r#"Command::new(std::env::current_exe().unwrap()).spawn();"#,
        r#"let executable = std::env::current_exe().unwrap();
            Command::new(executable).spawn();"#,
        r#"fn first() { let output = std::env::current_exe().unwrap(); }
            fn second() { json!({"limit":output}); }"#,
        r#"let destination = root.join("target/debug/fixture");
            std::fs::write(destination, b"not an executable path");"#,
        r#"json!({"target":{"kind":"module"}});"#,
        r#"let executable = ckdev_binary(std::env::current_exe().unwrap(), scratch_root());
            let quoted = executable.to_string_lossy();
            std::fs::write(script, format!("exec '{quoted}'"));"#,
        r#"json!({"program":ckdev_binary(root.join("target/debug/fake"), scratch_root())});"#,
        r#"// std::fs::write(script, std::env::current_exe().unwrap());
            let explanation = "current_exe()";"#,
        r##"let example = r#"json!({"program":std::env::current_exe().unwrap()});"#;"##,
    ] {
        assert!(
            indirect_build_path_sites(source).is_empty(),
            "rejected safe launch: {source}"
        );
    }
}

#[test]
fn rig_scripts_and_configs_never_launch_build_directory_executables() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let mut scanned = 0;
    let mut offenders = Vec::new();
    for file in files {
        if file.ends_with("crates/broca-test-support/tests/ckdev_guard.rs") {
            continue;
        }
        let source = std::fs::read_to_string(&file).unwrap();
        if !is_rig_source(&file, &source) {
            continue;
        }
        scanned += 1;
        let relative = file.strip_prefix(&workspace).unwrap_or(&file);
        offenders.extend(
            indirect_build_path_sites(&source)
                .into_iter()
                .map(|site| format!("{}: {site}", relative.display())),
        );
    }
    assert!(scanned > 0, "the scan must cover test and rig source files");
    assert!(
        offenders.is_empty(),
        "indirect launches from build paths:\n{}",
        offenders.join("\n")
    );
}
