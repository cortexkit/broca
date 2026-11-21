use futures::executor::block_on;

use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Id128;
use crate::frame::FrameKey;
use crate::keys::Cipher;

cipher_tests!(
    appends_build_a_tree_whose_lookups_find_every_entry,
    an_out_of_order_entry_refuses,
    ordinal_lookups_walk_a_deep_tree,
    a_range_walk_reads_only_the_pages_that_overlap_it,
    a_page_naming_itself_below_refuses_every_walk,
);

use super::{DirEntry, DirectoryError, Pages};

fn entry(n: u64) -> DirEntry {
    DirEntry {
        object: format!("seg-{n}"),
        first_seq: n * 10,
        last_seq: n * 10 + 9,
        size: 1,
        sealed_at_ms: n,
        first_ordinal: None,
        last_ordinal: None,
    }
}

/// Appending many entries through a small fanout builds a tree several levels deep. Every entry
/// is found by any seq inside it, the root names them all, and each append replaces only pages
/// on the old rightmost path, never one the new root still names.
fn appends_build_a_tree_whose_lookups_find_every_entry(cipher: Cipher) {
    block_on(appends_build_a_tree(cipher))
}

async fn appends_build_a_tree(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("a".into());
    backend.write_head(&addr, b"h", None, 1).await.unwrap();
    let key = FrameKey::for_tests(cipher, 1, &[7; 32]);
    let pages = Pages {
        backend: &backend,
        addr: &addr,
        session: Id128([1; 16]),
        key: &key,
        fanout: 3,
    };
    let mut root = None;
    let mut retired = Vec::new();
    for n in 1..=40u64 {
        let appended = pages
            .append(root.as_ref(), entry(n), &format!("c{n}/d"), 1)
            .await
            .unwrap();
        retired.extend(appended.replaced);
        root = Some(appended.root);
    }
    let root = root.unwrap();
    // 40 entries at 3 per page need 4 levels (27 < 40 <= 81).
    assert_eq!(root.level, 3);
    assert_eq!((root.first_seq, root.last_seq), (10, 409));
    for n in 1..=40u64 {
        for seq in [n * 10, n * 10 + 9] {
            assert_eq!(pages.find(Some(&root), seq).await.unwrap(), Some(entry(n)));
        }
    }
    assert_eq!(pages.find(Some(&root), 5).await.unwrap(), None);
    // Every page the final tree names is still stored, and none of them was ever retired.
    let mut stack = vec![root];
    while let Some(page) = stack.pop() {
        assert!(
            !retired.contains(&page.object),
            "{} is named and retired",
            page.object
        );
        stack.extend(pages.read(&page).await.unwrap().children);
    }
    // An entry that doesn't follow the last one refuses.
    let last = pages.find(None, 0).await.unwrap();
    assert!(last.is_none());
}

fn an_out_of_order_entry_refuses(cipher: Cipher) {
    block_on(out_of_order(cipher))
}

async fn out_of_order(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("a".into());
    backend.write_head(&addr, b"h", None, 1).await.unwrap();
    let key = FrameKey::for_tests(cipher, 1, &[7; 32]);
    let pages = Pages {
        backend: &backend,
        addr: &addr,
        session: Id128([1; 16]),
        key: &key,
        fanout: 3,
    };
    let root = pages.append(None, entry(2), "c1/d", 1).await.unwrap().root;
    assert!(matches!(
        pages.append(Some(&root), entry(1), "c2/d", 1).await,
        Err(DirectoryError::OutOfOrder)
    ));
}

/// Lookups by ordinal walk a tree several levels deep too. Each entry places four messages,
/// every third places none (ordinals absent), and pages carry the ordinal range of what they
/// hold, so the walk takes the one child that holds the ordinal.
fn ordinal_lookups_walk_a_deep_tree(cipher: Cipher) {
    block_on(async {
        let backend = MemoryBackend::default();
        let addr = SessionAddr("a".into());
        backend.write_head(&addr, b"h", None, 1).await.unwrap();
        let key = FrameKey::for_tests(cipher, 1, &[7; 32]);
        let pages = Pages {
            backend: &backend,
            addr: &addr,
            session: Id128([1; 16]),
            key: &key,
            fanout: 3,
        };
        let mut root = None;
        let mut placed = 0u64;
        let mut expected = Vec::new();
        for n in 1..=40u64 {
            let mut item = entry(n);
            if n % 3 != 0 {
                item.first_ordinal = Some(placed + 1);
                item.last_ordinal = Some(placed + 4);
                placed += 4;
            }
            expected.push(item.clone());
            root = Some(
                pages
                    .append(root.as_ref(), item, &format!("c{n}/d"), 1)
                    .await
                    .unwrap()
                    .root,
            );
        }
        let root = root.unwrap();
        assert_eq!(
            (root.first_ordinal, root.last_ordinal),
            (Some(1), Some(placed))
        );
        for item in expected.iter().filter(|item| item.first_ordinal.is_some()) {
            for ordinal in [item.first_ordinal.unwrap(), item.last_ordinal.unwrap()] {
                assert_eq!(
                    pages
                        .find_ordinal(Some(&root), ordinal)
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(item),
                    "ordinal {ordinal}"
                );
            }
        }
        assert_eq!(pages.find_ordinal(Some(&root), 0).await.unwrap(), None);
        assert_eq!(
            pages.find_ordinal(Some(&root), placed + 1).await.unwrap(),
            None
        );
    });
}

/// A range of entries early in a deep tree reads only the pages on the paths to those entries,
/// none of the pages after the range, and finds exactly the entries the range touches.
fn a_range_walk_reads_only_the_pages_that_overlap_it(cipher: Cipher) {
    block_on(async {
        let backend = MemoryBackend::default();
        let addr = SessionAddr("a".into());
        backend.write_head(&addr, b"h", None, 1).await.unwrap();
        let key = FrameKey::for_tests(cipher, 1, &[7; 32]);
        let pages = Pages {
            backend: &backend,
            addr: &addr,
            session: Id128([1; 16]),
            key: &key,
            fanout: 3,
        };
        let mut root = None;
        for n in 1..=40u64 {
            root = Some(
                pages
                    .append(root.as_ref(), entry(n), &format!("c{n}/d"), 1)
                    .await
                    .unwrap()
                    .root,
            );
        }
        let root = root.unwrap();
        let before = backend.object_reads();
        let found = pages.entries_between(Some(&root), 15, 25).await.unwrap();
        let read = backend.object_reads() - before;
        assert_eq!(found, vec![entry(1), entry(2)]);
        // Four levels: the root, then one page per level down to the leaf holding entries 1-3.
        assert_eq!(read, 4, "the range read {read} pages");
        let before = backend.object_reads();
        assert_eq!(pages.entries_from(Some(&root), 15).await.unwrap().len(), 40);
        assert!(
            backend.object_reads() - before > 20,
            "the walk to the tip reads every page"
        );
    });
}

/// A damaged directory whose root names itself as its own child, one level down, refuses every
/// lookup instead of looping. Each lookup reads through one memo, and the memo already holds the
/// root at its real level when the child reference names it again. Each walk runs on its own
/// thread with a deadline, so a regression fails here rather than hanging the suite.
fn a_page_naming_itself_below_refuses_every_walk(cipher: Cipher) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        block_on(self_naming_root(cipher));
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("a walk of a self-naming directory never ended");
}

async fn self_naming_root(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("a".into());
    backend.write_head(&addr, b"h", None, 1).await.unwrap();
    let key = FrameKey::for_tests(cipher, 1, &[7; 32]);
    let session = Id128([1; 16]);
    let pages = Pages {
        backend: &backend,
        addr: &addr,
        session,
        key: &key,
        fanout: 3,
    };
    let child = super::PageRef {
        object: "p0".into(),
        level: 1,
        first_seq: 0,
        last_seq: 100,
        count: 1,
        first_ordinal: Some(0),
        last_ordinal: Some(100),
    };
    let root_page = super::Page {
        level: 2,
        entries: Vec::new(),
        children: vec![child.clone()],
    };
    use crate::codec::Message as _;
    let sealed = key.seal_unit(&super::aad(&session, "p0"), &root_page.encode());
    backend.put_object(&addr, "p0", &sealed, 1).await.unwrap();
    let root = super::PageRef { level: 2, ..child };
    assert!(matches!(
        pages.find(Some(&root), 50).await,
        Err(DirectoryError::Corrupt(_))
    ));
    assert!(matches!(
        pages.find_before(Some(&root), 50).await,
        Err(DirectoryError::Corrupt(_))
    ));
    assert!(matches!(
        pages.find_ordinal(Some(&root), 50).await,
        Err(DirectoryError::Corrupt(_))
    ));
}
