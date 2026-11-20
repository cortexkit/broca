//! The last-known-good catalog on disk: read at boot, written after each accepted refresh.

use std::fs;
use std::io::Write;
use std::path::Path;

use broca_owner_only::OpenOptions;

use super::{loaded, CatalogRefreshError, CatalogSource, FusiformCatalogResponse, RowExclusion};

pub(super) fn load_lkg(path: &Path) -> Result<Option<FusiformCatalogResponse>, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("decode {}: {error}", path.display()))
}

/// Persists `response` as the LKG, as a restart would load it: each row `excluded` says is still
/// served from the replaced catalog keeps the LKG's earlier row, so a restart serves it too, and
/// a document a restart couldn't load at all (one that `open` would refuse) is refused here and
/// the earlier LKG stays.
pub(super) fn persist_bootable_lkg(
    path: &Path,
    response: &FusiformCatalogResponse,
    excluded: &[RowExclusion],
) -> Result<(), CatalogRefreshError> {
    let mut persisted = std::borrow::Cow::Borrowed(response);
    let kept: Vec<&str> = excluded
        .iter()
        .filter(|row| row.kept_previous)
        .map(|row| row.model.as_str())
        .collect();
    if !kept.is_empty() {
        if let Ok(Some(previous)) = load_lkg(path) {
            let persisted = persisted.to_mut();
            for identity in kept {
                if let Some(row) = previous.models.get(identity) {
                    persisted.models.insert(identity.to_owned(), row.clone());
                }
            }
        }
    }
    loaded(&persisted, CatalogSource::LastKnownGood, None)?;
    persist_lkg(path, &persisted)
}

pub(super) fn persist_lkg(
    path: &Path,
    response: &FusiformCatalogResponse,
) -> Result<(), CatalogRefreshError> {
    let bytes = serde_json::to_vec(response)
        .map_err(|error| CatalogRefreshError::Serialize(error.to_string()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let create_parent = broca_owner_only::create_dir_all(parent);
    create_parent.map_err(|error| CatalogRefreshError::Persist {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("catalog-lkg"),
        std::process::id()
    ));
    let write_result = (|| {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        let mut file = options.open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp);
        return Err(CatalogRefreshError::Persist {
            path: path.to_path_buf(),
            message: error.to_string(),
        });
    }
    Ok(())
}
