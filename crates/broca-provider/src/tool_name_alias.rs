//! Shared tool-name allocation and provider-specific admission validation.
pub use broca_protocol::tool_name_alias::*;

#[cfg(test)]
thread_local! {
    static MAP_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_map_builds() {
    MAP_BUILDS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn map_builds() -> usize {
    MAP_BUILDS.with(std::cell::Cell::get)
}

/// The tool-name map a request rendered under `frozen` uses, for the tools `tools` and the calls
/// in `prompt`: the operator's aliases, then names allocated under the family's grammar. The
/// renderer and the transport, which restores echoed names, both take it from here, so they
/// always agree.
pub fn map_for(
    frozen: &crate::wire::FrozenRenderConfig,
    tools: &[broca_protocol::request::Tool],
    prompt: &[broca_protocol::content::Message],
) -> ToolNameMap {
    #[cfg(test)]
    MAP_BUILDS.with(|count| count.set(count.get() + 1));
    ToolNameMap::build_with_aliases_in(
        tools,
        prompt,
        &frozen.tool_name_aliases,
        grammar_for(frozen.family_id),
    )
}

/// The tool-name grammar of wire family `family`: Gemini and Code Assist, which sends Gemini's
/// request body, take only a letter or an underscore first.
pub fn grammar_for(family: crate::spec::WireFamilyId) -> NameGrammar {
    match family {
        crate::spec::WireFamilyId::Gemini | crate::spec::WireFamilyId::GoogleCodeAssist => {
            NameGrammar::LetterFirst
        }
        _ => NameGrammar::Any,
    }
}

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
