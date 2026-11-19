//! broca's review of a proposed settings view, before the daemon approves it.
//!
//! The schema's own rules run first ([`subc_config::Schema::review_tuple`]). Then every reader
//! that builds part of broca at start reads the effective document, so a view that would stop
//! broca at its next start is refused now, naming the key it fails at. A refusal carries the
//! key, never the value.

use serde_json::Value;
use subc_config::{
    Layer, LayerValue, Refusal, RefusalCode, ReviewedTuple, Schema, ValidationContext,
};

use super::Settings;

/// A reader that builds part of broca from the settings, and the top-level keys it reads.
struct Reader {
    keys: &'static [&'static str],
    check: fn(&Settings) -> Result<(), String>,
}

macro_rules! reader {
    ([$($key:literal),+], $reader:ty) => {
        Reader {
            keys: &[$($key),+],
            check: |settings| <$reader>::from_settings(settings).map(drop),
        }
    };
}

/// Every reader built at start. `chatgpt_force_https` has no reader here: it is a plain
/// boolean, and the schema already checks that.
const READERS: &[Reader] = &[
    reader!(
        ["account_selection"],
        crate::account_routing::AccountSelectionConfig
    ),
    reader!(
        ["anthropic_fallbacks"],
        crate::config::AnthropicFallbackConfig
    ),
    reader!(["backup_classes"], crate::session_class::BackupClasses),
    reader!(["billing"], crate::billing_routing::ChargeBasisTable),
    reader!(["bus_run_events_enabled"], crate::bus_events::Config),
    reader!(["cache"], crate::CacheTtlTable),
    reader!(
        ["classifier_routing"],
        crate::config::ClassifierRoutingConfig
    ),
    reader!(
        ["compaction_signals"],
        crate::config::CompactionSignalsConfig
    ),
    reader!(
        ["context_management"],
        crate::context_management::ContextManagementConfig
    ),
    reader!(["import"], crate::config::ImportConfig),
    reader!(
        ["llm_tools", "private_content_providers"],
        crate::config::LlmToolsConfig
    ),
    reader!(
        ["max_same_account_wait"],
        crate::config::AccountLimitPauseConfig
    ),
    reader!(["providers"], crate::provider_overrides::ProviderOverrides),
    reader!(["service_tier"], crate::config::ServiceTierConfig),
    reader!(["session_storage"], crate::config::SessionStorageConfig),
    reader!(["storage_sync"], crate::config::StorageSyncConfig),
    reader!(["tool_name_aliases"], crate::config::ToolNameAliasesConfig),
    reader!(["wal_recovery"], crate::config::WalRecoveryConfig),
];

/// Keys the schema alone checks. Only the coverage test reads this list.
#[cfg(test)]
const SCHEMA_ONLY: &[&str] = &["chatgpt_force_https"];

/// Review the user layer `user`: the effective view and where each value came from, or the
/// first refusal.
pub fn review(schema: &Schema, user: &Value) -> Result<ReviewedTuple, Refusal> {
    let context = ValidationContext::new("broca", None);
    let tuple = schema.review_tuple(&[LayerValue::new(Layer::User, user.clone())], &context)?;
    let settings = Settings::from_view(&tuple.effective, "managed settings");
    for reader in READERS {
        if let Err(error) = (reader.check)(&settings) {
            // The reader's message can quote the value, so it stays in broca's own log.
            tracing::warn!(target: "config", keys = ?reader.keys, %error, "settings view refused");
            let key = reader
                .keys
                .iter()
                .find(|key| user.get(**key).is_some())
                .unwrap_or(&reader.keys[0]);
            return Err(Refusal::new(
                RefusalCode::ValueInvalid,
                format!("/{key}"),
                None,
            ));
        }
    }
    Ok(tuple)
}

#[cfg(test)]
#[path = "review_tests.rs"]
mod tests;
