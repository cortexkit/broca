use serde::Deserialize;
use serde_json::value::RawValue;

/// Remove only the new run-usage members before comparing a reply with a pre-usage
/// golden. Token measurements and every unrelated byte must retain their old spelling.
pub fn legacy_bytes(actual: &[u8], golden: &[u8]) -> Vec<u8> {
    #[derive(Deserialize)]
    struct Body<'a> {
        #[serde(borrow)]
        usage: Option<&'a RawValue>,
        #[serde(borrow)]
        result: Option<&'a RawValue>,
    }
    let actual_body: Body<'_> = serde_json::from_slice(actual).unwrap();
    let golden_body: Body<'_> = serde_json::from_slice(golden).unwrap();
    let actual_body = actual_body
        .result
        .map(|value| serde_json::from_str(value.get()).unwrap())
        .unwrap_or(actual_body);
    let golden_body = golden_body
        .result
        .map(|value| serde_json::from_str(value.get()).unwrap())
        .unwrap_or(golden_body);
    if golden_body.usage.is_some_and(|usage| {
        let object: serde_json::Value = serde_json::from_str(usage.get()).unwrap();
        ["steps", "unreported_steps", "by_model"]
            .iter()
            .any(|key| object.get(key).is_some())
    }) {
        return actual.to_vec();
    }
    let Some(usage) = actual_body.usage else {
        return actual.to_vec();
    };
    let from = format!(",\"usage\":{}", usage.get());
    let to = if golden_body.usage.is_some() {
        let fields: std::collections::BTreeMap<&str, &RawValue> =
            serde_json::from_str(usage.get()).unwrap();
        let mut kept = usage.get().to_owned();
        for key in ["by_model", "steps", "unreported_steps"] {
            if let Some(value) = fields.get(key) {
                let member = format!("\"{key}\":{}", value.get());
                let with_comma = format!("{member},");
                let after_comma = format!(",{member}");
                let remove = if kept.contains(&with_comma) {
                    &with_comma
                } else if kept.contains(&after_comma) {
                    &after_comma
                } else {
                    &member
                };
                assert_eq!(kept.matches(remove).count(), 1);
                kept = kept.replacen(remove, "", 1);
            }
        }
        format!(",\"usage\":{kept}")
    } else {
        String::new()
    };
    let actual = std::str::from_utf8(actual).unwrap();
    assert_eq!(actual.matches(&from).count(), 1);
    actual.replacen(&from, &to, 1).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_usage_golden_is_not_normalized() {
        let golden =
            br#"{"state":"interrupted","usage":{"steps":1,"unreported_steps":1,"by_model":[]}}"#;
        assert_eq!(legacy_bytes(golden, golden), golden);
        let changed =
            br#"{"state":"interrupted","usage":{"steps":0,"unreported_steps":0,"by_model":[]}}"#;
        assert_eq!(legacy_bytes(changed, golden), changed);
        assert_ne!(legacy_bytes(changed, golden), golden);
    }

    #[test]
    fn normalization_keeps_legacy_usage_and_unrelated_bytes() {
        let golden = br#"{"state":"completed","usage":{"output_tokens":7,"input_tokens":12},"retries_used":2}"#;
        let actual = br#"{"state":"completed","usage":{"steps":3,"unreported_steps":1,"output_tokens":7,"input_tokens":12,"by_model":[{"steps":3}]},"retries_used":2}"#;
        assert_eq!(legacy_bytes(actual, golden), golden);
        let changed = br#"{"state":"completed","usage":{"steps":3,"output_tokens":8,"input_tokens":12,"unexpected":1},"retries_used":4}"#;
        assert_eq!(legacy_bytes(changed, golden), br#"{"state":"completed","usage":{"output_tokens":8,"input_tokens":12,"unexpected":1},"retries_used":4}"#);
    }
}
