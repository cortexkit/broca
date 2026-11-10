//! Shared tool-name allocation and provider-specific admission validation.
pub use broca_protocol::tool_name_alias::*;

/// Explicit wire names must already fit the selected family's grammar; unlike generated
/// grammar aliases, operator aliases are never repaired or suffixed to resolve collisions.
#[cfg(any(feature = "live", test))]
pub fn validate_operator_aliases(
    family: crate::spec::WireFamilyId,
    tools: &[broca_protocol::request::Tool],
    aliases: &std::collections::BTreeMap<String, String>,
) -> Result<(), broca_core::run::ToolAliasInvalid> {
    use crate::spec::WireFamilyId;
    use std::collections::BTreeSet;
    let declared: BTreeSet<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
    let mut occupied = BTreeSet::new();
    for (canonical, alias) in aliases {
        let reason = if alias.is_empty()
            || normalize(alias) != *alias
            || (matches!(
                family,
                WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist
            ) && !alias.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_'))
        {
            Some(format!(
                "alias {alias:?} is not a valid tool name for {family:?}"
            ))
        } else if declared.contains(alias.as_str()) && alias != canonical {
            Some(format!("alias {alias:?} equals another declared tool name"))
        } else if !occupied.insert(alias) {
            Some(format!("alias {alias:?} is used by another tool"))
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(broca_core::run::ToolAliasInvalid {
                tool: canonical.clone(),
                reason,
            });
        }
    }
    Ok(())
}
