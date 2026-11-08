use broca_protocol::BindIdentity;
use cortexkit_role_compaction::view::{View, ViewRefusal};
use serde_json::value::RawValue;

/// Only publication identity and policy presence are logged, never edits or content.
pub struct Publication {
    version: Option<u64>,
    built_through: bool,
    new_signed: bool,
}

impl Publication {
    pub fn from_view(view: &View) -> Self {
        Self {
            version: Some(view.version),
            built_through: view.built_through.is_some(),
            new_signed: view.new_signed.is_some(),
        }
    }

    pub fn from_raw(view: &RawValue) -> Self {
        let value: serde_json::Value = serde_json::from_str(view.get()).expect("RawValue is JSON");
        Self {
            version: value.get("version").and_then(serde_json::Value::as_u64),
            built_through: value.get("built_through").is_some(),
            new_signed: value.get("new_signed").is_some(),
        }
    }

    pub fn warn(&self, bind: &BindIdentity, code: &str) {
        tracing::warn!(
            target: "compaction",
            session_ref = %crate::agent_session_ref(bind),
            view_version = %self.version.map_or_else(|| "unknown".into(), |version| version.to_string()),
            refusal_code = code,
            built_through = self.built_through,
            new_signed = self.new_signed,
            "compaction published view refused"
        );
    }
}

pub fn refusal_code(refusal: &ViewRefusal) -> String {
    serde_json::to_value(refusal).expect("view refusal serializes")["code"]
        .as_str()
        .expect("view refusal has a code")
        .to_owned()
}
