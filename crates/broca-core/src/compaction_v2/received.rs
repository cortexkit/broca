//! View validation uses the normalized contract types, while durable edit
//! references compare the exact received JSON bytes, including unknown members.

use cortexkit_role_compaction::view::{Edit, View};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

#[derive(Clone, Debug)]
pub struct ReceivedView {
    view: View,
    edits: Vec<Box<RawValue>>,
}

impl ReceivedView {
    pub fn parse(raw: &RawValue) -> Result<Self, serde_json::Error> {
        if raw.get().len() > cortexkit_role_compaction::view::MAX_VIEW_BYTES {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "compaction view exceeds the maximum serialized size",
            ));
        }
        #[derive(Deserialize)]
        struct RawEdits {
            edits: Vec<Box<RawValue>>,
        }
        let view = serde_json::from_str(raw.get())?;
        let raw_edits: RawEdits = serde_json::from_str(raw.get())?;
        Ok(Self {
            view,
            edits: raw_edits.edits,
        })
    }

    pub fn rebuilt(
        version: u64,
        checksum: String,
        edits: Vec<Box<RawValue>>,
    ) -> Result<Self, String> {
        if version == 0 || version > 9_007_199_254_740_991 {
            return Err(format!(
                "compaction view version {version} is outside the protocol integer range"
            ));
        }
        let decoded: Vec<Edit> = edits
            .iter()
            .map(|raw| {
                serde_json::from_str(raw.get()).map_err(|error| {
                    format!("compaction view version {version} edit cannot decode: {error}")
                })
            })
            .collect::<Result<_, _>>()?;
        let view = View::seal(version, decoded);
        if view.checksum != checksum {
            return Err(format!(
                "compaction view version {version} checksum mismatch"
            ));
        }
        let received = Self { view, edits };
        received.check_size()?;
        Ok(received)
    }

    pub fn edits(&self) -> &[Box<RawValue>] {
        &self.edits
    }
    pub fn view(&self) -> &View {
        &self.view
    }

    pub fn check_size(&self) -> Result<(), String> {
        if self.view.version == 0 || self.view.version > 9_007_199_254_740_991 {
            return Err(format!(
                "compaction view version {} is outside the protocol integer range",
                self.view.version
            ));
        }
        if broca_protocol::serialized_json_bytes(self)
            > cortexkit_role_compaction::view::MAX_VIEW_BYTES as u64
        {
            return Err(format!(
                "compaction view version {} exceeds the maximum serialized size",
                self.view.version
            ));
        }
        Ok(())
    }
}

impl From<View> for ReceivedView {
    fn from(view: View) -> Self {
        let edits = view
            .edits
            .iter()
            .map(|edit| serde_json::value::to_raw_value(edit).expect("contract edit serializes"))
            .collect();
        Self { view, edits }
    }
}

impl PartialEq for ReceivedView {
    fn eq(&self, other: &Self) -> bool {
        self.view == other.view
            && self
                .edits
                .iter()
                .map(|edit| edit.get())
                .eq(other.edits.iter().map(|edit| edit.get()))
    }
}

impl Serialize for ReceivedView {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct RawView<'a> {
            version: u64,
            checksum: &'a str,
            edits: &'a [Box<RawValue>],
        }
        RawView {
            version: self.view.version,
            checksum: &self.view.checksum,
            edits: &self.edits,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReceivedView {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RawView {
            version: u64,
            checksum: String,
            edits: Vec<Box<RawValue>>,
        }
        let raw = RawView::deserialize(deserializer)?;
        Self::rebuilt(raw.version, raw.checksum, raw.edits).map_err(serde::de::Error::custom)
    }
}
