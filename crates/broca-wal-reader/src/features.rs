use crate::Error;

/// Billing readers may skip unknown content, but broca must understand every
/// feature before replaying a record into session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Billing,
    All,
}

/// Capability grammar: `[a-z0-9-]+/v` followed by one or more ASCII digits.
pub fn is_feature_name(name: &str) -> bool {
    let Some((stem, version)) = name.split_once("/v") else {
        return false;
    };
    !stem.is_empty()
        && stem
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_digit())
}

/// Returns whether the frame must be skipped. A refusal always wins over a skip
/// when a frame requires both unknown billing and unknown content features.
pub(crate) fn must_skip(
    requires: &[String],
    known: &[&str],
    policy: Policy,
) -> Result<bool, Error> {
    let unknown: Vec<&str> = requires
        .iter()
        .map(String::as_str)
        .filter(|name| !known.contains(name))
        .collect();
    let mut features: Vec<String> = unknown
        .iter()
        .filter(|name| policy == Policy::All || name.starts_with("bill-"))
        .map(|name| (*name).to_owned())
        .collect();
    features.sort_unstable();
    features.dedup();
    if !features.is_empty() {
        return Err(Error::NewerWriter { features });
    }
    Ok(!unknown.is_empty())
}
