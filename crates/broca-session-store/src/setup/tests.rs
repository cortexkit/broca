use futures::executor::block_on;

use crate::backend::{LocalBackend, MemoryBackend, SessionBackend};
use crate::codec::hex;
use crate::keys::DeploymentKey;
use crate::record::{PartKind, PartRef};

use super::{aad, digest, SetupError, SetupStore};

const TOOLS: &[u8] = br#"[{"name":"read","input_schema":{"type":"object"}}]"#;

/// A part round-trips, and a reader with an empty cache gets it from storage, checked against
/// its digest. Storing it again from anywhere writes nothing new.
#[test]
fn a_part_round_trips_and_is_stored_once() {
    let backend = MemoryBackend::default();
    let key = DeploymentKey::new(1, &[3; 32]).setup_key();
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
#[test]
fn missing_and_corrupt_parts_refuse() {
    let backend = MemoryBackend::default();
    let key = DeploymentKey::new(1, &[3; 32]).setup_key();
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
#[test]
fn local_files_store_each_part_once() {
    let root =
        std::env::temp_dir().join(format!("broca-setup-{}", hex(&crate::keys::random_id().0)));
    let backend = LocalBackend::new(&root);
    let key = DeploymentKey::new(1, &[3; 32]).setup_key();
    let part = block_on(SetupStore::new(&backend, &key).put(PartKind::Tools, TOOLS))
        .unwrap()
        .0;
    let path = root.join("setup").join(key.name(&part.digest));
    let first = std::fs::read(&path).unwrap();
    block_on(SetupStore::new(&backend, &key).put(PartKind::Tools, TOOLS)).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), first);
    let entries: Vec<_> = std::fs::read_dir(root.join("setup"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries.len(), 1, "no temporary file left: {entries:?}");
    assert_eq!(
        &*block_on(SetupStore::new(&backend, &key).get(&part)).unwrap(),
        TOOLS
    );
    for bad in ["", "../x", "ABCD", "a/b"] {
        assert!(block_on(backend.put_setup(bad, b"x")).is_err(), "{bad}");
        assert!(block_on(backend.get_setup(bad)).is_err(), "{bad}");
    }
    std::fs::remove_dir_all(root).unwrap();
}
