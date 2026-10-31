//! Admission-time vault metadata. Unknown observations never imply a default provider or plan.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// The vault's exact record, including only billing facts, not token payloads or account profiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedVaultFacts {
    pub credential_id: String,
    pub record_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_tier_source: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A failed metadata observation can still retain the id/version from the vault's token resolve reply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultFactsIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_version: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Frozen at run admission, or when auth recovery selects a different credential.
/// Ordinary resume and same-credential refresh retain it; absence means it was not captured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum VaultBillingFacts {
    Observed(ObservedVaultFacts),
    Unwitnessed(VaultFactsIdentity),
    Unavailable(VaultFactsIdentity),
    NotListed(VaultFactsIdentity),
    Ambiguous(VaultFactsIdentity),
    VersionMismatch(VaultFactsIdentity),
    InvalidMetadata(VaultFactsIdentity),
    /// Preserve future states as unknown rather than treating them as an observed plan.
    #[serde(untagged)]
    Unknown {
        state: String,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
}

/// A frozen observation is usable only when the served token's vault resolve reply names the same id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultFactsAttribution<'a> {
    Observed(&'a ObservedVaultFacts),
    Unknown(&'a VaultBillingFacts),
    WitnessMissing,
    WitnessMismatch,
}

impl VaultBillingFacts {
    pub fn attribution(&self, served_credential_id: Option<&str>) -> VaultFactsAttribution<'_> {
        let Self::Observed(facts) = self else {
            return VaultFactsAttribution::Unknown(self);
        };
        match served_credential_id.filter(|id| !id.trim().is_empty()) {
            None => VaultFactsAttribution::WitnessMissing,
            Some(id) if id != facts.credential_id => VaultFactsAttribution::WitnessMismatch,
            Some(_) => VaultFactsAttribution::Observed(facts),
        }
    }
}

fn nonblank(value: &str) -> bool {
    !value.trim().is_empty()
}

impl<'de> Deserialize<'de> for VaultBillingFacts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = Value::deserialize(deserializer)?;
        let state = value
            .get("state")
            .and_then(Value::as_str)
            .filter(|state| nonblank(state))
            .ok_or_else(|| {
                serde::de::Error::custom("vault billing facts require a nonempty state")
            })?
            .to_owned();
        if state == "observed" {
            let facts: ObservedVaultFacts =
                serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
            if !nonblank(&facts.credential_id)
                || facts
                    .provider_ids
                    .as_ref()
                    .is_some_and(|ids| ids.is_empty() || ids.iter().any(|id| !nonblank(id)))
                || facts
                    .plan_tier
                    .as_deref()
                    .is_some_and(|tier| !nonblank(tier))
                || facts
                    .plan_tier_source
                    .as_deref()
                    .is_some_and(|source| !nonblank(source))
            {
                return Err(serde::de::Error::custom(
                    "empty observed vault billing fact",
                ));
            }
            // `state` belongs to the enum, not its extension map.
            let mut facts = facts;
            facts.extra.remove("state");
            return Ok(Self::Observed(facts));
        }
        value
            .as_object_mut()
            .expect("a state requires an object")
            .remove("state");
        let identity = || -> Result<VaultFactsIdentity, D::Error> {
            let identity: VaultFactsIdentity =
                serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
            if identity
                .credential_id
                .as_deref()
                .is_some_and(|id| !nonblank(id))
            {
                return Err(serde::de::Error::custom(
                    "empty vault billing credential identity",
                ));
            }
            for name in ["provider_ids", "plan_tier", "plan_tier_source"] {
                if identity.extra.contains_key(name) {
                    return Err(serde::de::Error::custom(
                        "unknown vault observation carries facts",
                    ));
                }
            }
            Ok(identity)
        };
        Ok(match state.as_str() {
            "unwitnessed" => Self::Unwitnessed(identity()?),
            "unavailable" => Self::Unavailable(identity()?),
            "not_listed" => Self::NotListed(identity()?),
            "ambiguous" => Self::Ambiguous(identity()?),
            "version_mismatch" => Self::VersionMismatch(identity()?),
            "invalid_metadata" => Self::InvalidMetadata(identity()?),
            _ => Self::Unknown {
                state,
                extra: value
                    .as_object()
                    .expect("a state requires an object")
                    .clone(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshots_preserve_observed_unknown_and_future_states() {
        for state in [
            "unwitnessed",
            "unavailable",
            "not_listed",
            "ambiguous",
            "version_mismatch",
            "invalid_metadata",
        ] {
            let value = json!({"state": state, "credential_id":"vault-id", "record_version":232});
            let facts: VaultBillingFacts = serde_json::from_value(value.clone()).unwrap();
            assert!(matches!(
                facts.attribution(Some("vault-id")),
                VaultFactsAttribution::Unknown(_)
            ));
            assert_eq!(serde_json::to_value(facts).unwrap(), value);
        }
        let value = json!({"state":"future_reason", "future_field": [1,2]});
        let facts: VaultBillingFacts = serde_json::from_value(value.clone()).unwrap();
        assert!(matches!(facts, VaultBillingFacts::Unknown { .. }));
        assert_eq!(serde_json::to_value(facts).unwrap(), value);
        let value = json!({"state":"observed", "credential_id":"vault-id", "record_version":232,
            "provider_ids":["anthropic", "claude-code"], "plan_tier":"future-tier", "plan_tier_source":"future-source", "future_field":7});
        let facts: VaultBillingFacts = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            facts.attribution(None),
            VaultFactsAttribution::WitnessMissing
        );
        assert_eq!(
            facts.attribution(Some("other")),
            VaultFactsAttribution::WitnessMismatch
        );
        assert!(matches!(
            facts.attribution(Some("vault-id")),
            VaultFactsAttribution::Observed(_)
        ));
        assert_eq!(serde_json::to_value(facts).unwrap(), value);
    }

    #[test]
    fn unknown_never_decodes_as_empty_observed_facts() {
        for value in [
            json!({"state":"observed", "record_version":232}),
            json!({"state":"observed", "credential_id":"id"}),
            json!({"state":"observed", "credential_id":"", "record_version":232}),
            json!({"state":"observed", "credential_id":"id", "record_version":232, "provider_ids":[]}),
            json!({"state":"observed", "credential_id":"id", "record_version":232, "plan_tier":""}),
            json!({"state":"unavailable", "plan_tier":"max_5x"}),
            json!({"state":"unavailable", "credential_id":""}),
            json!({"state":""}),
            json!(null),
        ] {
            assert!(
                serde_json::from_value::<VaultBillingFacts>(value.clone()).is_err(),
                "{value}"
            );
        }
        let facts: VaultBillingFacts = serde_json::from_value(
            json!({"state":"observed", "credential_id":"id", "record_version":232}),
        )
        .unwrap();
        let VaultBillingFacts::Observed(facts) = facts else {
            panic!("observed")
        };
        assert_eq!(facts.provider_ids, None);
        assert_eq!(facts.plan_tier, None);
        assert_eq!(facts.plan_tier_source, None);
    }
}
