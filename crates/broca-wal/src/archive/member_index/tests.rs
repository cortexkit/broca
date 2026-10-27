use super::*;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "member-index-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn write(&self, stamp: u64, members: &[(u64, &[u8])]) -> PathBuf {
        let path = self.0.join(format!("fold-{stamp}.ark"));
        let mut writer = ArchiveWriter::create(&path)
            .unwrap()
            .without_feature_floor();
        for (addr, payload) in members {
            writer.append(&format!("{addr:016x}"), payload).unwrap();
        }
        writer.seal().unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        invalidate(&self.0);
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn read(paths: &[PathBuf], addr: u64) -> Option<Vec<u8>> {
    read_newest_member(paths, &format!("{addr:016x}"), 1000)
        .unwrap()
        .map(|member| member.payload)
}

#[test]
fn many_addresses_open_only_their_newest_container() {
    let fixture = Fixture::new();
    let mut paths = Vec::new();
    for stamp in 1..=20 {
        let members = (0..25)
            .map(|i| (stamp * 25 + i, b"payload".as_slice()))
            .collect::<Vec<_>>();
        paths.push(fixture.write(stamp, &members));
    }
    let before = expiry_io::counts().0;
    assert_eq!(read(&paths, 25).unwrap(), b"payload");
    assert!(
        expiry_io::counts().0 - before <= 21,
        "build opens each index at most once"
    );
    for addr in 25..525 {
        let before = expiry_io::counts().0;
        assert_eq!(read(&paths, addr).unwrap(), b"payload");
        assert_eq!(expiry_io::counts().0 - before, 1, "address {addr}");
    }
    let before = expiry_io::counts().0;
    assert!(read(&paths, 9999).is_none());
    assert_eq!(
        expiry_io::counts().0 - before,
        0,
        "a proven miss opens no container"
    );
}

#[test]
fn a_new_fold_explicitly_invalidates_locations() {
    let fixture = Fixture::new();
    let old = fixture.write(1, &[(1, b"old")]);
    assert_eq!(read(std::slice::from_ref(&old), 1).unwrap(), b"old");
    assert!(cache().lock().unwrap().contains_key(&fixture.0));
    let new = fixture.write(2, &[(1, b"new")]);
    assert!(!cache().lock().unwrap().contains_key(&fixture.0));
    assert_eq!(read(&[old, new], 1).unwrap(), b"new");
}

#[test]
fn restore_invalidates_only_its_address_and_other_members_still_open_once() {
    let fixture = Fixture::new();
    let mut paths = vec![fixture.write(1, &[(1, b"restore"), (2, b"archived")])];
    for stamp in 2..=20 {
        paths.push(fixture.write(stamp, &[(stamp + 100, b"unrelated")]));
    }
    let payload = read(&paths, 1).unwrap();
    std::fs::write(fixture.0.join("restored.wal"), &payload).unwrap();
    invalidate_restored_member(&fixture.0, "0000000000000001");
    assert!(cache().lock().unwrap().contains_key(&fixture.0));
    let before = expiry_io::counts().0;
    assert_eq!(read(&paths, 1).unwrap(), payload);
    assert_eq!(
        expiry_io::counts().0 - before,
        20,
        "restored address uses the scan"
    );
    let before = expiry_io::counts().0;
    assert_eq!(read(&paths, 2).unwrap(), b"archived");
    assert_eq!(
        expiry_io::counts().0 - before,
        1,
        "other member retains its indexed location"
    );
}

#[test]
fn deleted_container_falls_back_instead_of_serving_stale_bytes() {
    let fixture = Fixture::new();
    let old = fixture.write(1, &[(1, b"old")]);
    let new = fixture.write(2, &[(1, b"new"), (2, b"unique")]);
    let paths = [old, new.clone()];
    assert_eq!(read(&paths, 1).unwrap(), b"new");
    std::fs::remove_file(&new).unwrap();
    assert_eq!(read(&paths, 1).unwrap(), b"old");
    assert!(read(&paths, 2).is_none());
}

#[test]
fn replaced_container_and_rewritten_negative_are_detected() {
    for replace in [true, false] {
        let fixture = Fixture::new();
        let external = Fixture::new();
        let old = fixture.write(1, &[(1, b"old")]);
        let newer = fixture.write(2, &[(2, b"other")]);
        let paths = [old, newer.clone()];
        assert_eq!(read(&paths, 1).unwrap(), b"old");
        assert!(read(&paths, 3).is_none());
        // The writer uses another directory, so publication cannot invoke the local
        // invalidation hook. This models an external replace or in-place rewrite.
        let replacement = external.write(2, &[(1, b"new"), (3, b"added")]);
        if replace {
            std::fs::rename(replacement, &newer).unwrap();
        } else {
            std::fs::write(&newer, std::fs::read(replacement).unwrap()).unwrap();
        }
        assert_eq!(read(&paths, 1).unwrap(), b"new");
        assert_eq!(read(&paths, 3).unwrap(), b"added");
    }
}

#[test]
fn replacing_the_selected_container_does_not_serve_its_cached_location() {
    let fixture = Fixture::new();
    let external = Fixture::new();
    let selected = fixture.write(1, &[(1, b"old")]);
    let paths = [selected.clone()];
    assert_eq!(read(&paths, 1).unwrap(), b"old");
    std::fs::rename(external.write(1, &[(2, b"new")]), selected).unwrap();
    assert!(read(&paths, 1).is_none());
    assert_eq!(read(&paths, 2).unwrap(), b"new");
}

#[test]
fn changed_inventory_falls_back_before_rebuilding() {
    let fixture = Fixture::new();
    let external = Fixture::new();
    let old = fixture.write(1, &[(1, b"old")]);
    assert_eq!(read(std::slice::from_ref(&old), 1).unwrap(), b"old");
    let new = fixture.0.join("fold-2.ark");
    std::fs::rename(external.write(2, &[(1, b"new")]), &new).unwrap();
    let paths = [old.clone(), new];
    assert_eq!(read(&paths, 1).unwrap(), b"new");
    assert!(!cache().lock().unwrap().contains_key(&fixture.0));
    assert_eq!(read(&paths, 1).unwrap(), b"new");
    assert_eq!(read(&[old], 1).unwrap(), b"old");
}

#[test]
fn damaged_newest_payload_is_not_replaced_by_older_copy() {
    use std::io::{Seek, Write};
    let fixture = Fixture::new();
    let old = fixture.write(1, &[(1, b"old")]);
    let new = fixture.write(2, &[(1, b"new")]);
    let paths = [old, new.clone()];
    assert_eq!(read(&paths, 1).unwrap(), b"new");
    let mut file = std::fs::OpenOptions::new().write(true).open(new).unwrap();
    file.seek(SeekFrom::Start(MEMBER_HEADER_LEN)).unwrap();
    file.write_all(b"bad").unwrap();
    assert!(read_newest_member(&paths, "0000000000000001", 1000).is_err());
}
