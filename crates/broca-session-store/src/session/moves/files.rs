use super::*;
use crate::backend::MemoryBackend;
use crate::codec::Id128;
use std::io::Write as _;

impl Export {
    /// Opens the entire graph in memory before staging. In particular an authenticated head
    /// cannot be used to smuggle unrelated files into the receiver's class root.
    pub async fn validate(
        &self,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<(), Error> {
        self.check(addr, deployment)?;
        let memory = MemoryBackend::default();
        let (_, version) = self.head(addr, deployment)?;
        let head_path = format!("heads/{}/head", addr.0);
        let stored = &self.files[&head_path];
        memory.write_head(addr, &stored[8..], None, version).await?;
        for (path, bytes) in &self.files {
            if let Some(log) = path.strip_prefix(&format!("logs/{}/", addr.0)) {
                let id = parse_id(log)?;
                memory.set_log(addr, &id, bytes.clone());
            } else if let Some(name) = path.strip_prefix(&format!("objects/{}/", addr.0)) {
                memory.set_object(addr, name, bytes.clone());
            } else if let Some(name) = path.strip_prefix("setup/") {
                memory.set_setup(name, bytes.clone());
            } else if let Some(name) = path.strip_prefix("setup-plain/") {
                memory.set_setup(&format!("plain/{name}"), bytes.clone());
            }
        }
        let (mut reached, finding) =
            super::super::restore::reachable(&memory, addr, deployment, &stored[8..], version)
                .await
                .map_err(invalid)?;
        if let Some(finding) = finding {
            return Err(invalid(finding));
        }
        reached.insert(head_path, stored.clone());
        reached.insert(format!("heads/{}/v/{version}", addr.0), stored.clone());
        if reached != self.files {
            return Err(invalid("export has unreachable files"));
        }
        Ok(())
    }

    /// Writes only authenticated stored bytes, under owner-only directories. The caller keeps
    /// this folder private to its cut and holds the session lease until activation or abort.
    pub async fn stage(
        &self,
        root: &Path,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<(), Error> {
        self.validate(addr, deployment).await?;
        for (path, bytes) in &self.files {
            publish(root, path, bytes)?;
        }
        let staged = LocalBackend::new(root);
        if let Some(finding) = super::super::restore::check(&staged, addr, deployment)
            .await
            .map_err(invalid)?
        {
            return Err(invalid(finding));
        }
        Ok(())
    }

    /// Publishes setup first, then session data, and the authoritative head last. Repeating
    /// after a crash accepts an already-renamed file only when its bytes match the cut.
    pub fn activate(&self, staged: &Path, root: &Path, addr: &SessionAddr) -> Result<(), Error> {
        let head = format!("heads/{}/head", addr.0);
        let paths = self
            .files
            .keys()
            .filter(|path| path.starts_with("setup/") || path.starts_with("setup-plain/"))
            .chain(self.files.keys().filter(|path| {
                !path.starts_with("setup/") && !path.starts_with("setup-plain/") && **path != head
            }))
            .chain(std::iter::once(&head));
        for path in paths {
            let bytes = &self.files[path];
            let destination = checked_path(root, path)?;
            let source = checked_path(staged, path)?;
            let setup = path.starts_with("setup/") || path.starts_with("setup-plain/");
            if setup && destination.exists() {
                continue;
            }
            match std::fs::read(&source) {
                Ok(actual) if actual == *bytes => {
                    let parent = destination.parent().unwrap();
                    create_dirs(parent)?;
                    std::fs::rename(&source, &destination)?;
                    sync(parent)?;
                    sync(source.parent().unwrap())?;
                }
                Ok(_) => return Err(invalid(format!("staged file changed: {path}"))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if std::fs::read(&destination)? != *bytes {
                        return Err(invalid(format!("activated file changed: {path}")));
                    }
                    sync(destination.parent().unwrap())?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

fn parse_id(value: &str) -> Result<Id128, Error> {
    if value.len() != 32 {
        return Err(invalid("log id length"));
    }
    let mut bytes = [0; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).map_err(invalid)?;
    }
    Ok(Id128(bytes))
}

fn checked_path(root: &Path, path: &str) -> Result<std::path::PathBuf, Error> {
    let mut full = root.to_path_buf();
    // Check existing ancestors as well as leaves; create_dir_all follows links otherwise.
    if root
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.is_symlink())
    {
        return Err(invalid("staging root is a symlink"));
    }
    for component in path.split('/') {
        full.push(component);
        match full.symlink_metadata() {
            Ok(metadata) if metadata.is_symlink() => return Err(invalid("move path is a symlink")),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(full)
}

fn sync(path: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn create_dirs(path: &Path) -> Result<(), Error> {
    let mut directories = Vec::new();
    let mut at = Some(path);
    while let Some(path) = at {
        directories.push(path);
        if path.exists() {
            break;
        }
        at = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
    }
    broca_owner_only::create_dir_all(path)?;
    for directory in directories {
        sync(directory)?;
    }
    Ok(())
}

fn publish(root: &Path, path: &str, bytes: &[u8]) -> Result<(), Error> {
    let path = checked_path(root, path)?;
    let parent = path.parent().unwrap();
    create_dirs(parent)?;
    let temp = path.with_file_name(format!(".move-{}", hex(&crate::keys::random_id().0)));
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp, &path)?;
    sync(parent)?;
    Ok(())
}

/// Fences the live source while preserving the immutable head version exported by the cut.
pub async fn freeze(
    store: &LocalBackend,
    addr: &SessionAddr,
    version: Version,
    cut_id: &str,
    deployment: &DeploymentKey,
) -> Result<(), Error> {
    let (bytes, at) = store
        .read_head(addr)
        .await?
        .ok_or_else(|| invalid("source head missing"))?;
    let (mut head, key) = head::open(&bytes, deployment).map_err(invalid)?;
    match &head.residence {
        Residence::Moving(held) if held == cut_id => return Ok(()),
        Residence::Local(()) if at == version => {}
        _ => return Err(invalid("source head is not the pinned local head")),
    }
    head.residence = Residence::Moving(cut_id.into());
    store
        .write_head(addr, &head::seal(&head, &key), Some(at), at + 1)
        .await?;
    Ok(())
}

pub async fn release(
    store: &LocalBackend,
    addr: &SessionAddr,
    cut_id: &str,
    holder: &str,
    deployment: &DeploymentKey,
) -> Result<(), Error> {
    let (bytes, at) = store
        .read_head(addr)
        .await?
        .ok_or_else(|| invalid("source head missing"))?;
    let (mut head, key) = head::open(&bytes, deployment).map_err(invalid)?;
    match &head.residence {
        Residence::Moved(moved) if moved.cut_id == cut_id && moved.holder == holder => {
            return Ok(())
        }
        Residence::Moving(held) if held == cut_id => {}
        _ => return Err(invalid("release does not own the freeze")),
    }
    head.residence = Residence::Moved(MoveResidence {
        cut_id: cut_id.into(),
        holder: holder.into(),
    });
    store
        .write_head(addr, &head::seal(&head, &key), Some(at), at + 1)
        .await?;
    Ok(())
}

pub async fn abort(
    store: &LocalBackend,
    addr: &SessionAddr,
    cut_id: &str,
    deployment: &DeploymentKey,
) -> Result<(), Error> {
    let Some((bytes, at)) = store.read_head(addr).await? else {
        return Ok(());
    };
    let (mut head, key) = head::open(&bytes, deployment).map_err(invalid)?;
    if matches!(&head.residence, Residence::Moving(held) if held == cut_id) {
        head.residence = Residence::Local(());
        store
            .write_head(addr, &head::seal(&head, &key), Some(at), at + 1)
            .await?;
    }
    Ok(())
}
