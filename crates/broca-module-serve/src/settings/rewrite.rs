//! Rewrites a `broca.jsonc` written in an older shape into the shapes of broca's settings
//! schema, in place, once.
//!
//! Under the daemon's managed settings the daemon, not broca, reads `broca.jsonc` and checks it
//! against the schema broca declares, so a file in an older shape would be refused there. The
//! fleet rule is that each module converts its own user file at startup, before it connects: the
//! edits change only the values that need a new shape and keep every comment outside them, and
//! the old bytes are saved in broca's data folder first. A file already in the schema's shapes
//! is left alone, so the rewrite happens once.
//!
//! The daemon reads the file only at `<config home>/cortexkit/broca.jsonc`, while broca kept it
//! in its own folder, `<config home>/cortexkit/broca/broca.jsonc`. [`move_old_file`] copies it
//! there once, rewritten, and leaves the old file as it was: a build from before the move still
//! reads the old path, and finds the settings it understands if broca is rolled back.
//!
//! [`convert`] decides the new document. This module turns the difference between the old and
//! the new document into span-preserving edits (`subc_jsonc`), checks that the edited text
//! reads back as exactly the converted document, and only then replaces the file. Any edit that
//! fails, or a text that reads back differently, writes nothing.

use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use super::convert::convert;

/// What a rewrite did.
#[derive(Debug, Clone, PartialEq)]
pub struct Rewritten {
    /// One line per key removed or rewritten, naming the key.
    pub notes: Vec<String>,
    /// Where the file's previous bytes were saved.
    pub saved: PathBuf,
}

/// Rewrites the file at `path` into the schema's shapes when it is written in an older one,
/// saving its previous bytes under `data_dir` first. `Ok(None)` when there is nothing to do: the
/// file is absent or already in the schema's shapes.
pub fn rewrite_old_file(path: &Path, data_dir: &Path) -> Result<Option<Rewritten>, String> {
    let original = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    let text = String::from_utf8(original.clone())
        .map_err(|error| format!("read {} as utf-8: {error}", path.display()))?;
    let Some((edited, notes)) = rewritten_text(&text)? else {
        return Ok(None);
    };
    let saved = save_previous(&original, data_dir)?;
    replace_file(path, edited.as_bytes())?;
    Ok(Some(Rewritten { notes, saved }))
}

/// What moving the file did.
#[derive(Debug, Clone, PartialEq)]
pub enum Moved {
    /// Neither file is there: no settings were ever written.
    Nothing,
    /// The file was already at the daemon's path, and was rewritten there when it needed it.
    /// `stale_old` names a file left at the old path, which nothing reads any more.
    InPlace {
        rewritten: Option<Rewritten>,
        stale_old: Option<PathBuf>,
    },
    /// The file at the old path was copied to the daemon's, rewritten when it needed it. The old
    /// file stays as it was, for builds from before the move.
    Moved { notes: Vec<String>, saved: PathBuf },
}

/// The settings file the daemon reads for broca under managed settings:
/// `<config home>/cortexkit/broca.jsonc`, with the config home resolved exactly as the daemon
/// resolves it. `Err` when that home would be relative, which the daemon refuses.
pub fn daemon_settings_file() -> Result<PathBuf, String> {
    let home = PathBuf::from(cortexkit_store_types::resolve_config_home());
    if !home.is_absolute() {
        return Err(format!(
            "the config home {} isn't an absolute path: set XDG_CONFIG_HOME or HOME",
            home.display()
        ));
    }
    Ok(home.join("cortexkit").join("broca.jsonc"))
}

/// Copies the settings file from `old` (broca's own folder) to `new` (the path the daemon
/// reads), rewritten into the schema's shapes, saving its previous bytes under `data_dir` first.
/// `old` is never changed, so a rollback to a build that reads it finds its settings. With a file
/// at `new` already, the move happened before: that file is rewritten in place if it needs it,
/// and `old` is named as stale, since edits to it no longer reach this build. A step that fails
/// stops before the next, and `new` is written only once the old bytes are saved.
pub fn move_old_file(old: &Path, new: &Path, data_dir: &Path) -> Result<Moved, String> {
    let present = |path: &Path| {
        path.try_exists()
            .map_err(|error| format!("check {}: {error}", path.display()))
    };
    if present(new)? {
        let rewritten = rewrite_old_file(new, data_dir)?;
        let stale_old = present(old)?.then(|| old.to_owned());
        return Ok(Moved::InPlace {
            rewritten,
            stale_old,
        });
    }
    let original = match std::fs::read(old) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Moved::Nothing),
        Err(error) => return Err(format!("read {}: {error}", old.display())),
    };
    let text = String::from_utf8(original.clone())
        .map_err(|error| format!("read {} as utf-8: {error}", old.display()))?;
    let (moved, notes) = rewritten_text(&text)?.unwrap_or((text, Vec::new()));
    let saved = save_previous(&original, data_dir)?;
    write_new_file(new, moved.as_bytes())?;
    Ok(Moved::Moved { notes, saved })
}

/// `text` with every value that needs a new shape rewritten, and the conversion's notes; `None`
/// when `text` is already in the schema's shapes.
pub(crate) fn rewritten_text(text: &str) -> Result<Option<(String, Vec<String>)>, String> {
    let old = read_object(text)?;
    let converted = convert(&old)?;
    if converted.notes.is_empty() && converted.document == old {
        return Ok(None);
    }
    let mut edits = Vec::new();
    diff(&mut Vec::new(), &old, &converted.document, &mut edits);
    let mut edited = text.to_owned();
    for edit in &edits {
        let path: Vec<&str> = edit.path().iter().map(String::as_str).collect();
        match edit {
            Edit::Remove(_) => subc_jsonc::remove_member(&mut edited, &path).map(drop),
            Edit::Set(_, value) => subc_jsonc::set_member(&mut edited, &path, value),
        }
        .map_err(|error| format!("rewrite {}: {error}", path.join(".")))?;
    }
    if read_object(&edited)? != converted.document {
        return Err("the rewritten file does not read back as the converted settings".into());
    }
    Ok(Some((edited, converted.notes)))
}

/// One change to a member, named by its key path from the top-level object.
enum Edit {
    Remove(Vec<String>),
    Set(Vec<String>, Value),
}

impl Edit {
    fn path(&self) -> &[String] {
        match self {
            Edit::Remove(path) | Edit::Set(path, _) => path,
        }
    }
}

/// The edits that turn the object `old` into `new`: a member `new` lacks is removed, a member
/// whose value differs is set, and an object member that is an object on both sides is compared
/// member by member, so only the deepest values that changed are rewritten and comments inside
/// the rest stay.
fn diff(
    path: &mut Vec<String>,
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    edits: &mut Vec<Edit>,
) {
    for key in old.keys().filter(|key| !new.contains_key(*key)) {
        edits.push(Edit::Remove(joined(path, key)));
    }
    for (key, value) in new {
        match (old.get(key), value) {
            (Some(before), after) if before == after => {}
            (Some(Value::Object(before)), Value::Object(after)) => {
                path.push(key.clone());
                diff(path, before, after, edits);
                path.pop();
            }
            _ => edits.push(Edit::Set(joined(path, key), value.clone())),
        }
    }
}

fn joined(path: &[String], key: &str) -> Vec<String> {
    let mut joined = path.to_vec();
    joined.push(key.to_owned());
    joined
}

fn read_object(text: &str) -> Result<Map<String, Value>, String> {
    let json = subc_jsonc::jsonc_to_json(text).map_err(|error| format!("parse: {error}"))?;
    match serde_json::from_str(&json).map_err(|error| format!("parse: {error}"))? {
        Value::Object(object) => Ok(object),
        _ => Err("the settings file must hold one object".into()),
    }
}

/// Saves `bytes` as a new owner-only file in `data_dir`, named for the time it was saved.
fn save_previous(bytes: &[u8], data_dir: &Path) -> Result<PathBuf, String> {
    use std::io::Write;
    broca_owner_only::create_dir_all(data_dir)
        .map_err(|error| format!("create {}: {error}", data_dir.display()))?;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let saved = data_dir.join(format!("broca.jsonc.before-settings-{millis}"));
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&saved)
        .map_err(|error| format!("save the old settings at {}: {error}", saved.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("save the old settings at {}: {error}", saved.display()))?;
    Ok(saved)
}

/// Writes `bytes` as the owner-only file `path`, which must not exist, in one rename, so a reader
/// finds the whole file or none.
fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let name = path
        .file_name()
        .ok_or_else(|| format!("{} names no file", path.display()))?;
    let mut staged_name = name.to_os_string();
    staged_name.push(format!(".move-{}", std::process::id()));
    let staged = path.with_file_name(staged_name);
    let write = || -> std::io::Result<()> {
        let mut file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&staged, path)
    };
    write().map_err(|error| {
        let _ = std::fs::remove_file(&staged);
        format!("write {}: {error}", path.display())
    })
}

/// Replaces the file at `path` with `bytes` in one rename, keeping its permissions, so a reader
/// sees the old file or the new one and never a part.
fn replace_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let permissions = std::fs::metadata(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?
        .permissions();
    let name = path
        .file_name()
        .ok_or_else(|| format!("{} names no file", path.display()))?;
    let mut staged_name = name.to_os_string();
    staged_name.push(format!(".rewrite-{}", std::process::id()));
    let staged = path.with_file_name(staged_name);
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&staged)?;
        file.write_all(bytes)?;
        file.set_permissions(permissions.clone())?;
        file.sync_all()?;
        std::fs::rename(&staged, path)
    };
    write().map_err(|error| {
        let _ = std::fs::remove_file(&staged);
        format!("write {}: {error}", path.display())
    })
}

#[cfg(test)]
#[path = "rewrite_tests.rs"]
mod tests;
