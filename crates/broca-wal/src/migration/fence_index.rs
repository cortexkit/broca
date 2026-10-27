//! A durable per-ref shortcut for pending cut intents. Cut JSON remains the
//! downgrade authority; an uninitialized reader falls back to scanning it.
use super::*;
use std::sync::{Mutex, MutexGuard};

static WRITE: Mutex<()> = Mutex::new(());
fn root(state_root: &Path) -> PathBuf {
    state_root.join("agent-data/write-fences")
}
fn ready(state_root: &Path) -> PathBuf {
    root(state_root).join("ready")
}
fn refs(state: &Value) -> io::Result<Vec<&str>> {
    let Some(intent) = state.get("intent").filter(|value| !value.is_null()) else {
        return Ok(Vec::new());
    };
    let op = intent
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("cut intent has no op"))?;
    if !matches!(op, "release" | "activate") {
        return Ok(Vec::new());
    }
    let refs = state
        .get("refs")
        .or_else(|| state.get("ref_set"))
        .or_else(|| {
            intent
                .get("request")
                .and_then(|request| request.get("refs"))
        })
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("cut fence has no refs"))?;
    let mut result = Vec::new();
    for reference in refs {
        let kind = reference
            .get("ref_kind")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("cut ref has no kind"))?;
        let value = reference
            .get("ref")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("cut ref has no value"))?;
        if kind == "broca-session" {
            if value.len() != 32 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(invalid("invalid cut session ref"));
            }
            result.push(value);
        }
    }
    Ok(result)
}
fn publish(state_root: &Path, cut_id: &str, state: &Value) -> io::Result<()> {
    for reference in refs(state)? {
        atomic_json(state_root, &root(state_root).join(reference), &cut_id)?;
    }
    Ok(())
}

pub(super) struct Publication<'a> {
    _lock: MutexGuard<'static, ()>,
    state_root: &'a Path,
    cut_id: String,
    keep: Vec<String>,
}
impl Publication<'_> {
    pub(super) fn finish(self) -> io::Result<()> {
        for entry in fs::read_dir(root(self.state_root))? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name == "ready"
                || name.ends_with(".tmp")
                || self.keep.iter().any(|reference| reference == name)
            {
                continue;
            }
            if read_json::<String>(&path)?.as_deref() == Some(&self.cut_id) {
                unlink(&path)?;
            }
        }
        Ok(())
    }
}

pub(super) fn before<'a>(
    state_root: &'a Path,
    path: &Path,
    bytes: &[u8],
) -> io::Result<Option<Publication<'a>>> {
    if path.parent() != Some(state_root.join("agent-data/cuts").as_path())
        || !fs::try_exists(&ready(state_root))?
    {
        return Ok(None);
    }
    let lock = WRITE.lock().unwrap_or_else(|error| error.into_inner());
    let state: Value = serde_json::from_slice(bytes).map_err(invalid)?;
    let cut_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid("invalid cut path"))?
        .to_owned();
    let keep = refs(&state)?.into_iter().map(str::to_owned).collect();
    // Fence first, intent second. A crash may over-fence, never expose bytes
    // whose activation/release intent is durable but whose fence is missing.
    publish(state_root, &cut_id, &state)?;
    Ok(Some(Publication {
        _lock: lock,
        state_root,
        cut_id,
        keep,
    }))
}

pub fn rebuild(state_root: &Path) -> io::Result<()> {
    rebuild_with_read_dir(state_root, |path| fs::read_dir(path))
}

fn rebuild_with_read_dir(
    state_root: &Path,
    mut read_dir: impl FnMut(&Path) -> io::Result<fs::ReadDir>,
) -> io::Result<()> {
    let _lock = WRITE.lock().unwrap_or_else(|error| error.into_inner());
    if fs::try_exists(&ready(state_root))? {
        unlink(&ready(state_root))?;
    }
    durable_dir(state_root, &root(state_root))?;
    let fences = root(state_root);
    for entry in read_dir(&fences)? {
        let Some(entry) = listed_boot_entry(&fences, entry)? else {
            continue;
        };
        unlink(&entry.path())?;
    }
    let cuts = state_root.join("agent-data/cuts");
    match read_dir(&cuts) {
        Ok(entries) => {
            for entry in entries {
                let Some(entry) = listed_boot_entry(&cuts, entry)? else {
                    continue;
                };
                let path = entry.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    continue;
                }
                // A listed cut is fencing authority, so losing it before reading must
                // fail closed rather than publish an incomplete index.
                let state: Value = read_json(&path)?
                    .ok_or_else(|| invalid("cut disappeared during fence initialization"))?;
                let cut_id = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| invalid("invalid cut path"))?;
                publish(state_root, cut_id, &state)?;
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    atomic_json(state_root, &ready(state_root), &true)
}

fn listed_boot_entry(
    directory: &Path,
    entry: io::Result<fs::DirEntry>,
) -> io::Result<Option<fs::DirEntry>> {
    match entry {
        Ok(entry) => Ok(Some(entry)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // An entry lost during enumeration contributes no path to scan, unlike a known
            // cut lost during reading, so skip it only while the scan root remains readable.
            fs::read_dir(directory)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn check(wal_root: &Path, reference: &str) -> io::Result<Option<Option<String>>> {
    let state_root = wal_root.parent().unwrap_or(Path::new("."));
    if !fs::try_exists(&ready(state_root))? {
        return Ok(None);
    }
    if reference.len() != 32 || !reference.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(None);
    }
    Ok(Some(read_json(&root(state_root).join(reference))?))
}

#[cfg(test)]
mod tests;
