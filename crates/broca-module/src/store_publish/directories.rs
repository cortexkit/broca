//! Process-local evidence for unchanged shared directory entries, not for later renames.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

use broca_wal::migration;

#[derive(Default)]
pub struct PublicationDirs {
    stores: BTreeMap<(PathBuf, PathBuf), Proof>,
    #[cfg(test)]
    initializations: usize,
}

struct Proof {
    #[cfg(unix)]
    directories: Vec<(PathBuf, std::fs::File)>,
}

impl Proof {
    fn capture(paths: &[PathBuf]) -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                directories: paths
                    .iter()
                    .map(|path| Ok((path.clone(), std::fs::File::open(path)?)))
                    .collect::<io::Result<_>>()?,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = paths;
            Ok(Self {})
        }
    }

    fn valid(&self) -> io::Result<bool> {
        // Broca exclusively owns the state root and never renames these shared directories.
        // Native identities catch removal/replacement; external rename-away-and-back while
        // the process runs is tampering outside the state root's durability contract.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            for (path, held) in &self.directories {
                let current = match std::fs::metadata(path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
                    Err(error) => return Err(error),
                };
                let held = held.metadata()?;
                if !current.is_dir() || (current.dev(), current.ino()) != (held.dev(), held.ino()) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        // Without a stable directory identity, sync the shared paths and their parents through
        // the state root again. Existence or timestamps cannot identify the entry we synced.
        #[cfg(not(unix))]
        {
            Ok(false)
        }
    }
}

impl PublicationDirs {
    pub fn ensure(&mut self, state_root: &Path, store: &Path) -> io::Result<()> {
        self.ensure_paths(state_root, store, &["logs", "objects", "heads"])
    }

    fn ensure_paths(
        &mut self,
        state_root: &Path,
        store: &Path,
        folders: &[&str],
    ) -> io::Result<()> {
        let root = state_root.canonicalize()?;
        let relative = store
            .strip_prefix(state_root)
            .map_err(|_| io::Error::other("class store is outside the state root"))?;
        let requested = root.join(relative);
        let store = match requested.canonicalize() {
            Ok(path) if path.starts_with(&root) => path,
            Ok(_) => {
                return Err(io::Error::other(
                    "class store resolves outside the state root",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => requested,
            Err(error) => return Err(error),
        };
        let key = (root.clone(), store.clone());
        if let Some(proof) = self.stores.get(&key) {
            if proof.valid()? {
                return Ok(());
            }
        }
        self.stores.remove(&key);
        let shared: Vec<_> = folders.iter().map(|folder| store.join(folder)).collect();
        for path in &shared {
            migration::durable_dir(&root, path)?;
        }
        let store = store.canonicalize()?;
        let key = (root.clone(), store.clone());
        let mut paths: Vec<PathBuf> = store
            .ancestors()
            .take_while(|path| path.starts_with(&root))
            .map(Path::to_path_buf)
            .collect();
        paths.extend(shared.iter().cloned());
        let proof = Proof::capture(&paths)?;
        #[cfg(unix)]
        if !proof.valid()? {
            return Err(io::Error::other(
                "class store directories changed during initialization",
            ));
        }
        // Held directory handles prevent inode reuse from turning a remove-and-replace into
        // a false match. No proof is serialized; restart syncs these paths and all their parents
        // through the state root again.
        self.stores.insert(key, proof);
        #[cfg(test)]
        {
            self.initializations += 1;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn directory_proofs_are_process_local_and_replacement_invalidates_them() {
        let root = broca_test_support::scratch_root().join(format!(
            "directory-proof-{}",
            broca_session_store::codec::hex(&broca_session_store::keys::random_id().0)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = root.join("session-store");
        let mut pass = PublicationDirs::default();
        pass.ensure(&root, &store).unwrap();
        assert_eq!(pass.initializations, 1);
        pass.ensure(&root, &store).unwrap();
        assert_eq!(
            pass.initializations, 1,
            "unchanged entries reuse their paid barriers"
        );
        for folder in ["logs", "objects", "heads"] {
            std::fs::rename(store.join(folder), store.join(format!("old-{folder}"))).unwrap();
            std::fs::create_dir(store.join(folder)).unwrap();
            pass.ensure(&root, &store).unwrap();
        }
        assert_eq!(pass.initializations, 4);
        std::fs::remove_dir(store.join("objects")).unwrap();
        pass.ensure(&root, &store).unwrap();
        assert_eq!(
            pass.initializations, 5,
            "missing shared directories lose the proof"
        );
        let mut restarted = PublicationDirs::default();
        restarted.ensure(&root, &store).unwrap();
        assert_eq!(
            restarted.initializations, 1,
            "visible directories do not survive as a proof"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
