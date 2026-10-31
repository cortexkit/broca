//! Model pages retain transcript/replacement coordinates and native messages. The serving
//! layer can render unrequested media as text without changing stored messages.

use serde::{Deserialize, Serialize};

pub use cortexkit_role_llm_runner::read::EntrySource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadView {
    Model,
}

/// Decode this type only after installing the compaction writer's serving operations.
/// Reader adapters keep using `ReadParams`, which refuses `view` rather than silently
/// answering a model-view request with a raw transcript.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WriterReadParams {
    #[serde(flatten)]
    pub read: crate::ReadParams,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<ReadView>,
}

impl<'de> Deserialize<'de> for WriterReadParams {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut params = serde_json::Value::deserialize(deserializer)?;
        let view = params
            .as_object_mut()
            .and_then(|fields| fields.remove("view"))
            .unwrap_or(serde_json::Value::Null);
        let view = serde_json::from_value(view).map_err(serde::de::Error::custom)?;
        // A flattened derived deserializer drops unknown keys. Decode the remaining
        // object through the original strict type so misspelled paging keys still refuse.
        let read = serde_json::from_value(params).map_err(serde::de::Error::custom)?;
        Ok(Self { read, view })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelEntry {
    pub source: EntrySource,
    pub message: broca_protocol::Message,
    /// One row per exposed transcript part, as on a transcript page, with a number only when
    /// the part has been counted by a tag-rendering session. Absent on a replacement entry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<crate::SessionReadTag>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelPage {
    pub messages: Vec<ModelEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_from_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<crate::CursorWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
}

impl ModelPage {
    /// Inline readers receive text placeholders, not stored media references. Other message
    /// content stays native so a media-free model page retains its existing bytes.
    pub fn render_inline_media(&mut self) {
        use broca_protocol::{ContentBlock, MediaMarkerReason, ToolOutput, ToolOutputPart};
        for entry in &mut self.messages {
            for block in &mut entry.message.content {
                match block {
                    ContentBlock::Media(media) => {
                        *block = ContentBlock::Text {
                            text: broca_protocol::content::media_not_shown_marker(
                                media,
                                MediaMarkerReason::NotRequested,
                            ),
                        }
                    }
                    ContentBlock::ToolResult(result) => {
                        if let ToolOutput::Parts { parts } = &result.output {
                            if parts
                                .iter()
                                .any(|part| matches!(part, ToolOutputPart::Media { .. }))
                            {
                                result.output = ToolOutput::Text {
                                    text: broca_protocol::content::media_parts_text(
                                        parts,
                                        MediaMarkerReason::NotRequested,
                                    ),
                                };
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

impl TryFrom<broca_core::ModelPage> for ModelPage {
    type Error = serde_json::Error;

    fn try_from(page: broca_core::ModelPage) -> Result<Self, Self::Error> {
        Ok(Self {
            messages: page
                .messages
                .into_iter()
                .map(|entry| {
                    let source: EntrySource = serde_json::from_value(entry.source)?;
                    let tags = match &source {
                        EntrySource::Message { ordinal, .. } => {
                            crate::tag_rows(*ordinal, &entry.message)
                        }
                        EntrySource::Replacement { .. } => Vec::new(),
                    };
                    Ok(ModelEntry {
                        source,
                        message: entry.message,
                        tags,
                    })
                })
                .collect::<Result<_, Self::Error>>()?,
            lineage_id: page.lineage_id,
            next_from_ordinal: page.next_from_ordinal,
            head: page.head.map(|cursor| crate::CursorWire {
                wal_seq: cursor.wal_seq,
                sub_index: cursor.sub_index,
            }),
            compaction_id: page.compaction_id,
            version: page.version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(text: &str) -> broca_protocol::Message {
        broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: text.into() }],
        )
    }

    #[test]
    fn model_page_keeps_shared_sources_and_native_message_bytes() {
        let value = json!({"messages":[
            {"source":{"kind":"replacement","compaction_id":"c1","version":3,"from_ordinal":0,"to_ordinal":12},
             "message":message("replacement")},
            {"source":{"kind":"message","ordinal":12,"mid":"m12"},
             "message":message("raw tail")}
        ],"compaction_id":"c1","version":3,"lineage_id":"lin-1",
          "head":{"wal_seq":20,"sub_index":0},"next_from_ordinal":13});
        let page: ModelPage = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(page).unwrap(), value);
        let empty: ModelPage = serde_json::from_value(json!({"messages":[]})).unwrap();
        assert_eq!(serde_json::to_string(&empty).unwrap(), "{\"messages\":[]}");
    }

    #[test]
    fn unknown_model_source_is_not_silently_skipped() {
        let value = json!({"messages":[{"source":{"kind":"future"},
            "message":message("tail")}]});
        assert!(serde_json::from_value::<ModelPage>(value).is_err());
    }

    #[test]
    fn model_view_params_are_strict_and_raw_request_bytes_stay_unchanged() {
        let params: WriterReadParams = serde_json::from_value(json!({"view":"model"})).unwrap();
        assert_eq!(params.view, Some(ReadView::Model));
        assert_eq!(
            serde_json::to_string(&crate::ReadParams::default()).unwrap(),
            "{}"
        );
        assert!(serde_json::from_value::<WriterReadParams>(json!({"view":"future"})).is_err());
        assert!(serde_json::from_value::<crate::ReadParams>(json!({"view":"model"})).is_err());
        assert!(serde_json::from_value::<WriterReadParams>(json!({"fromOrdinal":1})).is_err());
        assert_eq!(
            serde_json::to_string(&WriterReadParams::default()).unwrap(),
            "{}"
        );
    }
}
