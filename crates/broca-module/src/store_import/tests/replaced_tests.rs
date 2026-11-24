use super::*;

/// A producer that writes a newer export, and a side file, at the source's path while the import
/// is staging loses neither: the import deletes only the files it read.
#[test]
fn a_source_replaced_after_it_was_read_is_not_deleted() {
    let folder = scratch("replaced");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/thalamus-export-calls-v1/opencode2");
    let bytes = std::fs::read(fixtures.join("export.json")).unwrap();
    let path = folder.join("export.json");
    write(&path, &bytes);
    let roots = ImportRoots::new(vec![folder.clone()], Some(folder.clone()));
    let params = params(
        ImportHarness::OpenCode2,
        &path,
        format!("{:x}", Sha256::digest(&bytes)),
    );
    let (read, _) = read_source(&params, &roots).unwrap();
    // The newer export replaces the one read, as a producer's rename over it does.
    let mut newer = bytes.clone();
    newer.extend_from_slice(b"\n");
    let staged = folder.join("export.json.new");
    write(&staged, &newer);
    std::fs::rename(&staged, &path).unwrap();
    let calls = ck_session_import::head::calls_path(&path);
    write(
        &calls,
        &std::fs::read(fixtures.join("export.calls.json")).unwrap(),
    );
    delete_sources(&roots, ImportHarness::OpenCode2, &read);
    assert_eq!(
        std::fs::read(&path).ok(),
        Some(newer),
        "the newer export stays"
    );
    assert!(calls.exists(), "the side file the import never read stays");
    let _ = std::fs::remove_dir_all(&folder);
}
