use futures::executor::block_on;

use crate::backend::{LocalBackend, MemoryBackend, SessionBackend};
use crate::codec::hex;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{PartKind, PartRef};

use super::{aad, digest, SetupError, SetupKey, SetupStore, PLAIN_PREFIX};

const TOOLS: &[u8] = br#"[{"name":"read","input_schema":{"type":"object"}}]"#;

cipher_tests!(
    a_part_round_trips_and_is_stored_once,
    missing_and_corrupt_parts_refuse,
    local_files_store_each_part_once,
    load_all_returns_every_part_and_refuses_a_damaged_one,
);

/// The test deployment's setup-store keys for sessions in mode `cipher`.
fn setup_key(cipher: Cipher) -> SetupKey {
    DeploymentKey::new(1, &[3; 32])
        .setup_key_for(cipher)
        .expect("the test deployment key is loaded")
}

/// The folder of `root` a part named `name` is stored in on local files, and its file name.
fn local_path(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    match name.strip_prefix(PLAIN_PREFIX) {
        Some(file) => root.join("setup-plain").join(file),
        None => root.join("setup").join(name),
    }
}

/// A part round-trips, and a reader with an empty cache gets it from storage, checked against
/// its digest. Storing it again from anywhere writes nothing new.
fn a_part_round_trips_and_is_stored_once(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let key = setup_key(cipher);
    let writer = SetupStore::new(&backend, &key);
    let (part, wrote) = block_on(writer.put(PartKind::Tools, TOOLS)).unwrap();
    assert!(wrote);
    assert!(!block_on(writer.put(PartKind::Tools, TOOLS)).unwrap().1);
    assert_eq!(
        part,
        PartRef {
            kind: PartKind::Tools,
            digest: digest(TOOLS)
        }
    );
    let stored = backend.setup_names();
    assert_eq!(stored.len(), 1);
    let first = block_on(backend.get_setup(&stored[0])).unwrap();

    // Another process, with nothing cached, reads and stores the same part.
    let reader = SetupStore::new(&backend, &key);
    assert_eq!(&*block_on(reader.get(&part)).unwrap(), TOOLS);
    block_on(reader.put(PartKind::Tools, TOOLS)).unwrap();
    let fresh = SetupStore::new(&backend, &key);
    block_on(fresh.put(PartKind::Tools, TOOLS)).unwrap();
    assert_eq!(backend.setup_names(), stored);
    assert_eq!(block_on(backend.get_setup(&stored[0])).unwrap(), first);
}

/// A stored name is an HMAC of the digest under the deployment's naming key: it never shows the
/// digest, and another deployment stores the same part under another name.
#[test]
fn names_hide_the_digest_and_differ_per_deployment() {
    let one = DeploymentKey::new(1, &[3; 32]).setup_key();
    let two = DeploymentKey::new(1, &[4; 32]).setup_key();
    let part = digest(TOOLS);
    assert_eq!(one.name(&part), one.name(&part));
    assert_ne!(one.name(&part), two.name(&part));
    assert_ne!(one.name(&part), hex(&part.0));
    // A deployment can't read another's parts: it looks them up under its own names.
    let backend = MemoryBackend::default();
    block_on(SetupStore::new(&backend, &one).put(PartKind::Tools, TOOLS)).unwrap();
    let error = block_on(SetupStore::new(&backend, &two).get(&PartRef {
        kind: PartKind::Tools,
        digest: part,
    }))
    .unwrap_err();
    assert!(matches!(error, SetupError::Missing(_)), "{error:?}");
}

/// A part that isn't stored is missing, never empty. Stored bytes that fail to authenticate,
/// that were moved from another name, or that authenticate but don't match the digest the
/// record names, are corrupt.
fn missing_and_corrupt_parts_refuse(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let key = setup_key(cipher);
    let other = br#"{"plan":1}"#;
    let tools = block_on(SetupStore::new(&backend, &key).put(PartKind::Tools, TOOLS))
        .unwrap()
        .0;
    let plan = block_on(SetupStore::new(&backend, &key).put(PartKind::Plan, other))
        .unwrap()
        .0;
    let get = |part: &PartRef| block_on(SetupStore::new(&backend, &key).get(part));

    let absent = PartRef {
        kind: PartKind::Plan,
        digest: digest(b"never stored"),
    };
    assert!(matches!(get(&absent), Err(SetupError::Missing(_))));

    let tools_name = key.name(&tools.digest);
    let plan_name = key.name(&plan.digest);
    let tools_sealed = block_on(backend.get_setup(&tools_name)).unwrap().unwrap();
    let plan_sealed = block_on(backend.get_setup(&plan_name)).unwrap().unwrap();

    // A flipped byte.
    let mut flipped = tools_sealed.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    backend.set_setup(&tools_name, flipped);
    assert!(matches!(get(&tools), Err(SetupError::Corrupt(_))));

    // Another part's bytes under this name: its authenticated data names the other part.
    backend.set_setup(&tools_name, plan_sealed);
    assert!(matches!(get(&tools), Err(SetupError::Corrupt(_))));

    // Bytes sealed correctly under this name but holding other content: only the digest check
    // catches this.
    backend.set_setup(&tools_name, key.cipher.seal_unit(&aad(&tools_name), other));
    assert!(matches!(get(&tools), Err(SetupError::Corrupt(_))));

    backend.set_setup(&tools_name, tools_sealed);
    assert_eq!(&*get(&tools).unwrap(), TOOLS);
}

/// On local files, a part is written whole under the setup folder, a second store leaves the
/// first file's bytes alone, and a name that isn't hex refuses before touching the disk.
fn local_files_store_each_part_once(cipher: Cipher) {
    let root =
        std::env::temp_dir().join(format!("broca-setup-{}", hex(&crate::keys::random_id().0)));
    let backend = LocalBackend::new(&root);
    let key = setup_key(cipher);
    let part = block_on(SetupStore::new(&backend, &key).put(PartKind::Tools, TOOLS))
        .unwrap()
        .0;
    let path = local_path(&root, &key.name(&part.digest));
    let first = std::fs::read(&path).unwrap();
    block_on(SetupStore::new(&backend, &key).put(PartKind::Tools, TOOLS)).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), first);
    let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries.len(), 1, "no temporary file left: {entries:?}");
    assert_eq!(
        &*block_on(SetupStore::new(&backend, &key).get(&part)).unwrap(),
        TOOLS
    );
    for bad in [
        "",
        "../x",
        "ABCD",
        "a/b",
        "plain/",
        "plain/../x",
        "plain/ABCD",
        "plain/a/b",
    ] {
        assert!(block_on(backend.put_setup(bad, b"x")).is_err(), "{bad}");
        assert!(block_on(backend.get_setup(bad)).is_err(), "{bad}");
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// `load_all` returns every stored part with its digest, on memory and on local files, skipping
/// a temporary file a crashed write left; a part whose bytes or name don't check is damage.
fn load_all_returns_every_part_and_refuses_a_damaged_one(cipher: Cipher) {
    const PLAN: &[u8] = br#"{"tool_items":[]}"#;
    let root = std::env::temp_dir().join(format!(
        "broca-setup-all-{}",
        hex(&crate::keys::random_id().0)
    ));
    let local = LocalBackend::new(&root);
    let memory = MemoryBackend::default();
    let key = setup_key(cipher);
    let check = |backend: &dyn Fn() -> Vec<(crate::codec::Digest, std::sync::Arc<[u8]>)>| {
        let mut loaded = backend();
        loaded.sort_by_key(|part| part.0 .0);
        let mut expected = vec![(digest(TOOLS), TOOLS), (digest(PLAN), PLAN)];
        expected.sort_by_key(|part| part.0 .0);
        assert_eq!(loaded.len(), 2);
        for ((digest, bytes), (want_digest, want)) in loaded.iter().zip(expected) {
            assert_eq!(*digest, want_digest);
            assert_eq!(&**bytes, want);
        }
    };
    for kind_bytes in [(PartKind::Tools, TOOLS), (PartKind::Plan, PLAN)] {
        block_on(SetupStore::new(&local, &key).put(kind_bytes.0, kind_bytes.1)).unwrap();
        block_on(SetupStore::new(&memory, &key).put(kind_bytes.0, kind_bytes.1)).unwrap();
    }
    let folder = local_path(&root, &key.name(&digest(TOOLS)));
    std::fs::write(folder.parent().unwrap().join(".half.tmp"), b"partial").unwrap();
    check(&|| block_on(SetupStore::new(&local, &key).load_all()).unwrap());
    check(&|| block_on(SetupStore::new(&memory, &key).load_all()).unwrap());
    // A part copied under another part's name fails authentication, since its seal binds the
    // name it was stored under.
    let names = memory.setup_names();
    let other = block_on(memory.get_setup(&names[1])).unwrap().unwrap();
    memory.set_setup(&names[0], other);
    assert!(matches!(
        block_on(SetupStore::new(&memory, &key).load_all()),
        Err(SetupError::CorruptStored(_))
    ));
    // A part sealed under a name its digest doesn't give authenticates, and fails the name check.
    let misnamed = MemoryBackend::default();
    let name = key.name(&digest(PLAN));
    block_on(misnamed.put_setup(&name, &key.cipher.seal_unit(&aad(&name), TOOLS))).unwrap();
    assert!(matches!(
        block_on(SetupStore::new(&misnamed, &key).load_all()),
        Err(SetupError::CorruptStored(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

/// Plain sessions' parts are named under the published key: the same name in every deployment,
/// in their own name space, and never an encrypted part's name.
#[test]
fn plain_names_are_the_same_everywhere_and_never_an_encrypted_name() {
    let part = digest(TOOLS);
    let plain = SetupKey::plain();
    let name = plain.name(&part);
    assert!(name.starts_with(PLAIN_PREFIX), "{name}");
    for deployment in [
        DeploymentKey::new(1, &[3; 32]),
        DeploymentKey::new(2, &[4; 32]),
        DeploymentKey::unloaded(1),
    ] {
        let key = deployment.setup_key_for(Cipher::None).unwrap();
        assert_eq!(key.name(&part), name);
    }
    let encrypted = DeploymentKey::new(1, &[3; 32]).setup_key();
    assert!(!encrypted.name(&part).starts_with(PLAIN_PREFIX));
    assert_ne!(encrypted.name(&part)[..], name[PLAIN_PREFIX.len()..]);
    assert_ne!(name[PLAIN_PREFIX.len()..], hex(&part.0));
}

/// A plain store and an encrypted store over one backend each read only their own parts: the
/// same part is stored once in each name space, `load_all` lists only its own, and a part stored
/// in the other mode only is missing, never read in the wrong mode. On local files the plain
/// parts sit in a folder of their own, beside the encrypted ones.
#[test]
fn each_mode_reads_only_its_own_parts() {
    const PLAN: &[u8] = br#"{"tool_items":[]}"#;
    let root = std::env::temp_dir().join(format!(
        "broca-setup-modes-{}",
        hex(&crate::keys::random_id().0)
    ));
    let local = LocalBackend::new(&root);
    let memory = MemoryBackend::default();
    let encrypted = DeploymentKey::new(1, &[3; 32]).setup_key();
    let plain = SetupKey::plain();
    fn check<B: SessionBackend>(backend: &B, encrypted: &SetupKey, plain: &SetupKey) {
        let tools = block_on(SetupStore::new(backend, encrypted).put(PartKind::Tools, TOOLS))
            .unwrap()
            .0;
        block_on(SetupStore::new(backend, plain).put(PartKind::Tools, TOOLS)).unwrap();
        let plan = block_on(SetupStore::new(backend, plain).put(PartKind::Plan, PLAN))
            .unwrap()
            .0;
        let mut names = block_on(backend.list_setup()).unwrap();
        names.sort();
        let mut expected = vec![
            encrypted.name(&tools.digest),
            plain.name(&tools.digest),
            plain.name(&plan.digest),
        ];
        expected.sort();
        assert_eq!(names, expected);
        let loaded = |key: &SetupKey| {
            let mut digests: Vec<_> = block_on(SetupStore::new(backend, key).load_all())
                .unwrap()
                .into_iter()
                .map(|(digest, _)| digest)
                .collect();
            digests.sort_by_key(|digest| digest.0);
            digests
        };
        assert_eq!(loaded(encrypted), [tools.digest]);
        let mut both = vec![tools.digest, plan.digest];
        both.sort_by_key(|digest| digest.0);
        assert_eq!(loaded(plain), both);
        assert!(matches!(
            block_on(SetupStore::new(backend, encrypted).get(&plan)),
            Err(SetupError::Missing(_))
        ));
        assert_eq!(
            &*block_on(SetupStore::new(backend, plain).get(&plan)).unwrap(),
            PLAN
        );
    }
    check(&memory, &encrypted, &plain);
    check(&local, &encrypted, &plain);
    let file = |folder: &str| -> Vec<_> {
        std::fs::read_dir(root.join(folder))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect()
    };
    assert_eq!(file("setup").len(), 1, "the encrypted part");
    assert_eq!(file("setup-plain").len(), 2, "the plain parts");
    std::fs::remove_dir_all(root).unwrap();
}
