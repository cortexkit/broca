//! The wire types of a head's import into the new session storage: `session.import` with a file
//! source, and `session.install_view`, which records the compaction provider's first view of
//! the imported history.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Records the compaction view a moved head's provider already applies as the session's applied
/// view, before the session's first step.
pub const OP_SESSION_INSTALL_VIEW: &str = "session.install_view";

/// The host whose history a file source holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportHarness {
    /// OpenCode 1's SQLite database.
    #[serde(rename = "opencode1")]
    OpenCode1,
    /// An OpenCode 2 session export, written by Thalamus's `session.export`.
    #[serde(rename = "opencode2")]
    OpenCode2,
    /// A Pi session journal.
    #[serde(rename = "pi")]
    Pi,
}

/// Where an import's history is: a file broca reads, under one of its configured import roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSource {
    pub harness: ImportHarness,
    pub path: String,
    /// The host's id of the session the file holds.
    pub session: String,
    /// The file's SHA-256, in lowercase hex, checked before it is read.
    pub sha256: String,
}

/// Where the imported history came from, recorded on the seeded run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportOrigin {
    /// Always `import`.
    pub kind: String,
    pub source_ref: String,
}

/// `session.import` with a file source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileImportParams {
    /// The import's idempotency key: a retry with the same key and payload answers the stored
    /// result, and another payload under it is refused.
    pub import_id: String,
    pub source: ImportSource,
    pub origin: ImportOrigin,
    /// The highest part number the head has already shown. The imported messages' parts are
    /// numbered above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_floor: Option<u64>,
}

/// Which move-list shape a reply carries, decided by where the head was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveListShape {
    /// A head on the standalone OpenCode 1 plugin: rows carry `finished_summary`.
    Plugin,
    /// A head already on a runner (OpenCode 2, Pi): rows are `{mid, entry}`.
    Runner,
}

/// One source message and the entry it became.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRow {
    /// The source's id of the message.
    pub mid: String,
    /// The ordinal of the broca entry it became, or null where broca serves none.
    pub entry: Option<u64>,
    /// In the plugin shape only: summary is true/1 and finish is `stop`, regardless of role/error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_summary: Option<bool>,
    /// The row is a message broca added itself, which no source row is: the tool results split
    /// off an assistant row, or the results that close a call the source left unanswered. Its
    /// `mid` is broca's id, and it follows the source row it belongs to. Absent on source rows.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub added: bool,
}

/// The move list: one row per source message on the session's path, in source order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveList {
    pub shape: MoveListShape,
    pub rows: Vec<MoveRow>,
}

/// The last entry placed by this import, using the compaction entry's exact identity/digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportNewest {
    pub ordinal: u64,
    pub mid: String,
    pub digest: String,
}

/// The reply to a file-source `session.import`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileImportReply {
    pub moves: MoveList,
    pub newest: ImportNewest,
    pub history_version: u64,
    /// The session's incarnation in the new storage, in lowercase hex.
    pub incarnation: String,
    /// How many messages the import placed.
    pub imported_messages: u64,
    /// System messages left out: broca builds a run's system text from its plan.
    pub dropped_system_messages: u64,
}

/// `session.install_view`: the compaction view the moved head's compaction provider already
/// applies, exactly as the provider handed it over when the head moved.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallViewParams {
    pub view: Box<RawValue>,
}

/// The reply to `session.install_view`: the view the session now applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallViewReply {
    pub version: u64,
    pub checksum: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_import_shapes_require_all_newest_fields_and_omit_source_entry() {
        for shape in ["plugin", "runner"] {
            let json = serde_json::json!({"moves":{"shape":shape,"rows":[{"mid":"msg_synthetic","entry":1}]},
                "newest":{"ordinal":1,"mid":"msg_synthetic","digest":"synthetic-digest"},
                "history_version":0,"incarnation":"abcd","imported_messages":1,"dropped_system_messages":0});
            let decoded: FileImportReply = serde_json::from_value(json.clone()).unwrap();
            let encoded = serde_json::to_value(decoded).unwrap();
            assert_eq!(encoded, json);
            assert!(encoded["moves"]["rows"][0].get("source_entry").is_none());
            for field in ["ordinal", "mid", "digest"] {
                let mut missing = json.clone();
                missing["newest"].as_object_mut().unwrap().remove(field);
                assert!(serde_json::from_value::<FileImportReply>(missing).is_err());
            }
            let mut missing = json;
            missing.as_object_mut().unwrap().remove("newest");
            assert!(serde_json::from_value::<FileImportReply>(missing).is_err());
        }
    }

    #[test]
    fn file_import_params_round_trip_and_refuse_unknown_members() {
        let params: FileImportParams = serde_json::from_value(serde_json::json!({
            "import_id": "imp-1",
            "source": {"harness": "opencode2", "path": "/x/head.json", "session": "ses_1",
                       "sha256": "ab"},
            "origin": {"kind": "import", "source_ref": "thalamus:ses_1"},
            "tag_floor": 42
        }))
        .unwrap();
        assert_eq!(params.source.harness, ImportHarness::OpenCode2);
        assert_eq!(params.tag_floor, Some(42));
        assert!(
            serde_json::from_value::<FileImportParams>(serde_json::json!({
                "import_id": "imp-1",
                "source": {"harness": "codex", "path": "/x", "session": "s", "sha256": "ab"},
                "origin": {"kind": "import", "source_ref": "r"}
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<FileImportParams>(serde_json::json!({
                "import_id": "imp-1",
                "source": {"harness": "pi", "path": "/x", "session": "s", "sha256": "ab"},
                "origin": {"kind": "import", "source_ref": "r"},
                "messages": []
            }))
            .is_err()
        );
    }

    #[test]
    fn a_runner_move_row_has_no_finished_summary_member() {
        let row = MoveRow {
            mid: "m".into(),
            entry: None,
            finished_summary: None,
            added: false,
        };
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            serde_json::json!({"mid": "m", "entry": null})
        );
    }

    /// A message broca added says so; a source row carries no `added` member, and reads back
    /// as not added.
    #[test]
    fn only_an_added_move_row_carries_added() {
        let added = MoveRow {
            mid: "g1.7".into(),
            entry: Some(3),
            finished_summary: None,
            added: true,
        };
        let value = serde_json::to_value(&added).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"mid": "g1.7", "entry": 3, "added": true})
        );
        assert_eq!(serde_json::from_value::<MoveRow>(value).unwrap(), added);
        let source: MoveRow =
            serde_json::from_value(serde_json::json!({"mid": "m", "entry": 1})).unwrap();
        assert!(!source.added);
    }
}
