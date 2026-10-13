//! Provider prompt-cache hint emission helpers.

/// Convert the frozen OpenAI retention vocabulary to the current wire spelling.
/// The frozen policy keeps the spec's semantic value (`in-memory`); the API field
/// spells the same retention mode as `in_memory`.
pub(crate) fn openai_prompt_cache_retention_wire_value(retention: &str) -> &str {
    match retention {
        "in-memory" => "in_memory",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_retention_wire_spelling_maps_only_in_memory() {
        assert_eq!(
            openai_prompt_cache_retention_wire_value("in-memory"),
            "in_memory"
        );
        assert_eq!(openai_prompt_cache_retention_wire_value("24h"), "24h");
    }
}
