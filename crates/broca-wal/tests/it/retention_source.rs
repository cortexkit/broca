//! Retention writes are limited to the core run loop and session actor.
//! Real-daemon tests independently verify that session.send admits valid retention policies.

use super::compaction_source::{close_brace, is_cfg_test_module_file, sources, tokens};
use std::path::Path;

fn writes(source: &str) -> Vec<String> {
    let tokens = tokens(source);
    let mut found = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i..].starts_with(&["#", "[", "cfg", "(", "test", ")", "]"])
            || tokens[i] == "enum"
            || tokens[i] == "struct"
        {
            while i < tokens.len() && tokens[i] != ";" && tokens[i] != "{" {
                i += 1;
            }
            if tokens.get(i) == Some(&"{") {
                i = close_brace(&tokens, i);
            }
            i += 1;
            continue;
        }
        if ["RunStarted", "Queued", "Steered"].contains(&tokens[i])
            && tokens.get(i + 1) == Some(&"{")
        {
            let variant = tokens[i];
            let end = close_brace(&tokens, i + 1);
            let mut next = end + 1;
            while matches!(tokens.get(next), Some(&")" | &"]")) {
                next += 1;
            }
            let pattern = matches!(tokens.get(next), Some(&"=" | &"|" | &"if"));
            if !pattern {
                let mut field = i + 2;
                let mut depth = 0;
                for at in i + 2..=end {
                    if depth == 0 && matches!(tokens[at], "," | "}") {
                        let member = &tokens[field..at];
                        if let Some(name) = member.first() {
                            let gated = *name == "retention"
                                || (variant != "RunStarted" && *name == "ts_ms");
                            if gated && member != [*name, ":", "None"] {
                                found.push(format!("{variant}.{name}"));
                            }
                        }
                        field = at + 1;
                    }
                    match tokens[at] {
                        "(" | "[" | "{" => depth += 1,
                        ")" | "]" | "}" if depth > 0 => depth -= 1,
                        _ => {}
                    }
                }
            }
            i = end;
        }
        i += 1;
    }
    found
}

#[test]
fn retention_writes_are_limited_to_the_core_and_actor() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).unwrap() {
        let src = entry.unwrap().path().join("src");
        if src.is_dir() {
            sources(&src, &mut paths);
        }
    }
    assert!(paths.len() > 100, "scan must reach real production sources");
    let mut inventory = std::collections::BTreeMap::new();
    for path in paths.iter().filter(|path| !is_cfg_test_module_file(path)) {
        let source = std::fs::read_to_string(path).unwrap();
        let mut found = writes(&source);
        if !found.is_empty() {
            found.sort();
            let relative = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            inventory.insert(relative, found);
        }
    }
    let expected = std::collections::BTreeMap::from([
        (
            "crates/broca-core/src/run.rs".to_owned(),
            vec!["RunStarted.retention".to_owned()],
        ),
        (
            "crates/broca-module/src/actor.rs".to_owned(),
            vec![
                "Queued.retention".to_owned(),
                "Queued.ts_ms".to_owned(),
                "Steered.retention".to_owned(),
                "Steered.retention".to_owned(),
                "Steered.ts_ms".to_owned(),
                "Steered.ts_ms".to_owned(),
            ],
        ),
    ]);
    assert_eq!(inventory, expected, "retention writer inventory changed");
}

#[test]
fn retention_source_scan_detects_planted_writes_not_patterns_or_test_items() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let live = std::fs::read_to_string(root.join("crates/broca-core/src/run.rs")).unwrap();
    assert!(live.contains("WalRecord::RunStarted {"));
    for variant in ["RunStarted", "Queued", "Steered"] {
        for member in ["retention", "ts_ms"] {
            if variant == "RunStarted" && member == "ts_ms" {
                continue;
            }
            let expected = format!("{variant}.{member}");
            let planted = format!(
                "{live}\nfn planted() {{ WalRecord::{variant} {{ {member}: Some(3600) }}; }}"
            );
            let mut expected_live = writes(&live);
            expected_live.push(expected.clone());
            assert_eq!(writes(&planted), expected_live);
            let shorthand = format!("fn planted() {{ WalRecord::{variant} {{ {member} }}; }}");
            assert_eq!(writes(&shorthand), std::slice::from_ref(&expected));
            let absent = format!("fn legacy() {{ WalRecord::{variant} {{ {member}: None }}; }}");
            assert!(writes(&absent).is_empty());
            let patterns = format!("match record {{ WalRecord::{variant} {{ {member}: Some(n), .. }} => {{}} }}\nif let Some(WalRecord::{variant} {{ {member}, .. }}) = record {{}}\n#[cfg(test)] mod tests {{ fn example() {{ WalRecord::{variant} {{ {member}: Some(3600) }}; }} }}\nfn later() {{ WalRecord::{variant} {{ {member}: Some(3600) }}; }}");
            assert_eq!(writes(&patterns), [expected]);
        }
    }
    assert!(writes(r###"fn legacy() { let json = r##"WalRecord::Queued { retention: Some(3600) }"##; /* WalRecord::Steered { ts_ms: Some(5) } */ }"###).is_empty());
}
