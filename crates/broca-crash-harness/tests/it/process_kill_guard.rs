use std::path::Path;

broca_crash_harness::require_process_kill!();

fn test_owner(relative: &Path, source: &str) -> bool {
    let integration = relative.starts_with("tests")
        && !relative.starts_with("tests/scenarios")
        && relative != Path::new("tests/it/main.rs");
    integration
        || source.contains("#[test]")
        || source.contains("#[tokio::test")
        || relative.file_name().unwrap() == "tests.rs"
        || relative
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("_tests.rs")
}

#[test]
fn generated_test_files_cannot_escape_the_process_kill_inventory() {
    assert!(test_owner(
        Path::new("tests/new_file.rs"),
        "crash_harness_child!(lookup);"
    ));
    assert!(test_owner(
        Path::new("src/new_tests.rs"),
        "generated_tests!();"
    ));
    assert!(!test_owner(
        Path::new("tests/scenarios/mod.rs"),
        "fn scripts() {}"
    ));
}

#[test]
fn every_harness_test_file_requires_a_real_process_kill() {
    fn visit(root: &Path, path: &Path, owners: &mut usize) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, owners);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::fs::read_to_string(&path).unwrap();
                if test_owner(path.strip_prefix(root).unwrap(), &source) {
                    *owners += 1;
                    assert!(
                        source.lines().any(|line| matches!(line.trim(),
                            "broca_crash_harness::require_process_kill!();" | "crate::require_process_kill!();")),
                        "{} must invoke require_process_kill! so its test binary proves process death and resume",
                        path.display()
                    );
                }
            }
        }
    }
    let mut owners = 0;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    visit(root, &root.join("tests"), &mut owners);
    visit(root, &root.join("src"), &mut owners);
    assert!(
        owners >= 26,
        "the source inventory must visit the test owners"
    );
}
