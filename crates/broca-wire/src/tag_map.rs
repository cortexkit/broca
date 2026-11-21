//! `session.tag_map`: which message and part each part number (the number broca renders, between
//! section signs, in front of a numbered part) belongs to, for a session kept in the session store.
//!
//! Operator only, and outside the runner role: no caller-facing read carries tag rows. The reply
//! carries ids and numbers only, never any content.

use broca_protocol::content::Role;
use serde::{Deserialize, Serialize};

pub const OP_SESSION_TAG_MAP: &str = "session.tag_map";

/// How many messages a page holds when the request names no limit.
pub const TAG_MAP_DEFAULT_LIMIT: u64 = 500;

/// The most messages one page holds; a larger limit reads as this one.
pub const TAG_MAP_MAX_LIMIT: u64 = 2000;

/// `session.tag_map` request parameters. An absent or null params object reads the first page;
/// an unknown field is refused.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagMapParams {
    /// Start after the message with this ordinal (counting from 1); from the first without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ordinal: Option<u64>,
    /// How many messages the page reads, not rows: a message's parts are never split across
    /// pages. Defaults to [`TAG_MAP_DEFAULT_LIMIT`], held between 1 and [`TAG_MAP_MAX_LIMIT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

impl TagMapParams {
    /// The number of messages the page reads.
    pub fn page_limit(&self) -> u64 {
        self.limit
            .unwrap_or(TAG_MAP_DEFAULT_LIMIT)
            .clamp(1, TAG_MAP_MAX_LIMIT)
    }
}

/// One numbered part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagMapRow {
    /// The part's number, as the model is shown it.
    pub tag: u64,
    /// The message's id, as `session.read` and `session.entries` serve it.
    pub mid: String,
    /// The message's ordinal (counting from 1), as `session.read` and `session.entries` serve it.
    pub ordinal: u64,
    /// The zero-based index of the part's block within its message, which is the part's id in
    /// `session.entries`.
    pub part: u64,
    /// The message's role.
    pub role: Role,
}

/// The `session.tag_map` reply: one row per numbered part of the page's messages, in transcript
/// order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagMapResponse {
    /// True when the session's latest run shows the model part numbers, or when this page holds
    /// a part that an earlier run numbered. When false, `rows` is empty.
    pub numbered: bool,
    pub rows: Vec<TagMapRow>,
    /// The ordinal to pass as `after_ordinal` for the next page; present only when more messages
    /// follow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_ordinal: Option<u64>,
}

impl TagMapResponse {
    /// The reply for a session that numbers nothing.
    pub fn unnumbered() -> Self {
        Self {
            numbered: false,
            rows: Vec::new(),
            next_after_ordinal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_take_absent_fields_and_refuse_unknown_ones() {
        let empty: TagMapParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, TagMapParams::default());
        assert_eq!(empty.page_limit(), TAG_MAP_DEFAULT_LIMIT);
        let big: TagMapParams = serde_json::from_value(serde_json::json!({"limit": 9000})).unwrap();
        assert_eq!(big.page_limit(), TAG_MAP_MAX_LIMIT);
        let zero: TagMapParams = serde_json::from_value(serde_json::json!({"limit": 0})).unwrap();
        assert_eq!(zero.page_limit(), 1);
        assert!(
            serde_json::from_value::<TagMapParams>(serde_json::json!({"content": true})).is_err()
        );
    }

    #[test]
    fn a_last_page_names_no_next_ordinal() {
        let reply = TagMapResponse {
            numbered: true,
            rows: vec![TagMapRow {
                tag: 12,
                mid: "m1".into(),
                ordinal: 7,
                part: 0,
                role: Role::User,
            }],
            next_after_ordinal: None,
        };
        assert_eq!(
            serde_json::to_value(&reply).unwrap(),
            serde_json::json!({
                "numbered": true,
                "rows": [{"tag": 12, "mid": "m1", "ordinal": 7, "part": 0, "role": "user"}],
            })
        );
    }
}
