//! An old-shape `broca.jsonc` is rewritten once, in place, keeping its comments, with the old
//! bytes saved first; anything that can't be rewritten exactly writes nothing.

use serde_json::{json, Value};

use super::{move_old_file, rewrite_old_file, rewritten_text, Moved};

/// A file in today's shapes, as broca.jsonc on a long-running computer looks: obsolete keys,
/// an untagged alias row, comments inside and between members.
const OLD: &str = r#"{
  // Publish run events on the bus.
  "bus_run_events_enabled": true,
  "vault": {"module": "claustrum"}, // obsolete
  "wal_archive_idle_retention_days": 30,
  "tool_name_aliases": {
    // Claude Code's spellings.
    "oauth:anthropic": {"names": {"read": "Read"}},
  },
  "service_tier": {
    // Kept as written.
    "enabled": true
  },
  "max_same_account_wait": 30,
}
"#;

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root().join(format!(
        "rewrite-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn parsed(text: &str) -> Value {
    serde_json::from_str(&subc_jsonc::jsonc_to_json(text).unwrap()).unwrap()
}

#[test]
fn an_old_file_is_rewritten_once_keeping_its_comments() {
    let (text, notes) = rewritten_text(OLD)
        .unwrap()
        .expect("an old file is rewritten");
    assert_eq!(
        parsed(&text),
        json!({
            "bus_run_events_enabled": true,
            "tool_name_aliases": {"oauth:anthropic": {"kind": "names", "names": {"read": "Read"}}},
            "service_tier": {"enabled": true},
            "max_same_account_wait": {"kind": "minutes", "minutes": 30},
        })
    );
    for kept in [
        "// Publish run events on the bus.",
        "// Claude Code's spellings.",
        "// Kept as written.",
    ] {
        assert!(text.contains(kept), "{kept} is lost:\n{text}");
    }
    assert!(
        !text.contains("vault") && !text.contains("wal_archive"),
        "{text}"
    );
    assert_eq!(notes.len(), 4, "{notes:?}");
    assert_eq!(
        rewritten_text(&text).unwrap(),
        None,
        "a rewritten file is left alone"
    );
}

#[test]
fn the_file_is_replaced_and_its_old_bytes_saved_owner_only() {
    let dir = scratch("replace");
    let file = dir.join("broca.jsonc");
    std::fs::write(&file, OLD).unwrap();
    let data = dir.join("data");
    let done = rewrite_old_file(&file, &data).unwrap().expect("rewritten");
    assert_eq!(std::fs::read_to_string(&done.saved).unwrap(), OLD);
    assert!(done.saved.starts_with(&data), "{}", done.saved.display());
    let now = std::fs::read_to_string(&file).unwrap();
    assert_eq!(
        Some(now.clone()),
        rewritten_text(OLD).unwrap().map(|(text, _)| text)
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&done.saved).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{mode:o}");
    }
    assert_eq!(
        rewrite_old_file(&file, &data).unwrap(),
        None,
        "the second start does nothing"
    );
    assert_eq!(
        std::fs::read_dir(&data).unwrap().count(),
        1,
        "one saved copy"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_new_shape_or_absent_file_is_left_alone() {
    let dir = scratch("alone");
    let file = dir.join("broca.jsonc");
    let data = dir.join("data");
    assert_eq!(rewrite_old_file(&file, &data).unwrap(), None);
    let current =
        "{\n  // new shapes already\n  \"max_same_account_wait\": {\"kind\": \"max\"}\n}\n";
    std::fs::write(&file, current).unwrap();
    assert_eq!(rewrite_old_file(&file, &data).unwrap(), None);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), current);
    assert!(!data.exists(), "nothing is saved when nothing changes");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_value_the_converter_refuses_writes_nothing() {
    let dir = scratch("refused");
    let file = dir.join("broca.jsonc");
    let data = dir.join("data");
    let refused = "{\n  \"vault\": {},\n  \"max_same_account_wait\": \"forever\"\n}\n";
    std::fs::write(&file, refused).unwrap();
    let error = rewrite_old_file(&file, &data).unwrap_err();
    assert!(error.contains("max_same_account_wait"), "{error}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), refused);
    assert!(!data.exists(), "nothing is saved when nothing is written");
    std::fs::remove_dir_all(dir).unwrap();
}

/// A top-level `charge_basis` row moves under `billing.charge_basis`, which the file doesn't
/// have yet; the edits create the parents.
#[test]
fn a_moved_key_lands_under_parents_the_file_did_not_have() {
    let old = "{\n  // billing rows\n  \"charge_basis\": {\"openai:chatgpt\": \"subscription_included\"}\n}\n";
    let (text, _) = rewritten_text(old).unwrap().expect("the row moves");
    assert_eq!(
        parsed(&text),
        json!({"billing": {"charge_basis": {"openai": {"chatgpt": "subscription_included"}}}})
    );
    assert!(text.contains("// billing rows"), "{text}");
}

/// The file is copied from broca's folder to the daemon's path, rewritten, with its old bytes
/// saved owner-only. The old file stays as it was, for a rollback, and the next start names it as
/// stale and changes nothing.
#[test]
fn the_old_file_moves_to_the_daemons_path() {
    let dir = scratch("move");
    let old = dir.join("cortexkit/broca/broca.jsonc");
    let new = dir.join("cortexkit/broca.jsonc");
    std::fs::create_dir_all(old.parent().unwrap()).unwrap();
    std::fs::write(&old, OLD).unwrap();
    let data = dir.join("data");
    let Moved::Moved { notes, saved } = move_old_file(&old, &new, &data).unwrap() else {
        panic!("the file moves");
    };
    assert!(!notes.is_empty());
    assert_eq!(
        std::fs::read_to_string(&old).unwrap(),
        OLD,
        "the old file is unchanged"
    );
    assert_eq!(std::fs::read_to_string(&saved).unwrap(), OLD);
    assert_eq!(
        Some(std::fs::read_to_string(&new).unwrap()),
        rewritten_text(OLD).unwrap().map(|(text, _)| text)
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&new).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{mode:o}");
    }
    assert_eq!(
        move_old_file(&old, &new, &data).unwrap(),
        Moved::InPlace {
            rewritten: None,
            stale_old: Some(old.clone())
        }
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// The daemon's config folder need not exist yet: the move makes it.
#[test]
fn the_move_makes_the_daemons_config_folder() {
    let dir = scratch("move-folder");
    let old = dir.join("old/broca.jsonc");
    let new = dir.join("config/cortexkit/broca.jsonc");
    std::fs::create_dir_all(old.parent().unwrap()).unwrap();
    std::fs::write(&old, OLD).unwrap();
    assert!(matches!(
        move_old_file(&old, &new, &dir.join("data")).unwrap(),
        Moved::Moved { .. }
    ));
    assert_eq!(
        Some(std::fs::read_to_string(&new).unwrap()),
        rewritten_text(OLD).unwrap().map(|(text, _)| text)
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// With a file at both paths the new one is kept and the old one named as stale, even when the
/// old one is newer: edits to it no longer reach this build.
#[test]
fn a_file_at_both_paths_keeps_the_new_one() {
    let dir = scratch("both");
    let old = dir.join("old.jsonc");
    let new = dir.join("new.jsonc");
    let current = "{\n  \"max_same_account_wait\": {\"kind\": \"max\"}\n}\n";
    std::fs::write(&old, OLD).unwrap();
    std::fs::write(&new, current).unwrap();
    assert_eq!(
        move_old_file(&old, &new, &dir.join("data")).unwrap(),
        Moved::InPlace {
            rewritten: None,
            stale_old: Some(old.clone())
        }
    );
    assert_eq!(std::fs::read_to_string(&new).unwrap(), current);
    assert_eq!(std::fs::read_to_string(&old).unwrap(), OLD);
    assert_eq!(
        move_old_file(&dir.join("gone"), &dir.join("none"), &dir.join("data")).unwrap(),
        Moved::Nothing
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// A value the converter refuses moves nothing: the old file stays where it was.
#[test]
fn a_refused_value_moves_nothing() {
    let dir = scratch("refused-move");
    let old = dir.join("old.jsonc");
    let new = dir.join("new.jsonc");
    std::fs::write(&old, "{\"max_same_account_wait\": \"soon\"}").unwrap();
    assert!(move_old_file(&old, &new, &dir.join("data")).is_err());
    assert!(old.exists());
    assert!(!new.exists());
    std::fs::remove_dir_all(dir).unwrap();
}
