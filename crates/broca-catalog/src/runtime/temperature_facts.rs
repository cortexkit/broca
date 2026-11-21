//! Readers for the temperature facts of a fusiform row: whether the model takes a temperature,
//! and the bounds a requested one is clamped to.

use std::collections::BTreeMap;

use serde_json::Value;

use super::TEMPERATURE_FACTS;

pub(super) fn temperature_facts(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> broca_core::provider::TemperaturePolicy {
    let malformed = |key: &str| {
        tracing::warn!(
            model = identity,
            fact = key,
            "ignoring malformed temperature fact"
        )
    };
    let bound = |key: &str| {
        temperature_fact(facts.get(key), |value| {
            value.as_f64().filter(|value| value.is_finite())
        })
        .unwrap_or_else(|()| {
            malformed(key);
            None
        })
    };
    broca_core::provider::TemperaturePolicy {
        supported: temperature_fact(facts.get(TEMPERATURE_FACTS[0]), Value::as_bool)
            .unwrap_or_else(|()| {
                malformed(TEMPERATURE_FACTS[0]);
                None
            }),
        min: bound(TEMPERATURE_FACTS[1]),
        max: bound(TEMPERATURE_FACTS[2]),
    }
}

/// One temperature fact: absent and `null` both mean fusiform doesn't know it (fusiform serves
/// `null` for a flag its upstream withdrew), so neither is a value. Anything `parse` refuses is
/// malformed.
fn temperature_fact<T>(
    value: Option<&Value>,
    parse: impl Fn(&Value) -> Option<T>,
) -> Result<Option<T>, ()> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => parse(value).map(Some).ok_or(()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    /// A `null` temperature fact is unknown, like an absent one, and never malformed.
    #[test]
    fn a_null_temperature_fact_reads_as_absent() {
        let flag = Value::as_bool;
        assert_eq!(super::temperature_fact(Some(&Value::Null), flag), Ok(None));
        assert_eq!(super::temperature_fact(None, flag), Ok(None));
        assert_eq!(
            super::temperature_fact(Some(&Value::Bool(false)), flag),
            Ok(Some(false))
        );
        assert_eq!(
            super::temperature_fact(Some(&serde_json::json!("yes")), flag),
            Err(())
        );
    }
}
