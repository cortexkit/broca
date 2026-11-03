//! Stable message identifiers are independent of the public one-based ordinal.

/// A builder and parser share the canonical short lineage prefix.
fn lineage_prefix(lineage: Option<&str>) -> Option<&str> {
    lineage.and_then(|id| id.get(..8)).filter(|prefix| {
        prefix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

/// Build from the session's resolved mode and native stored position, not its public ordinal.
/// Legacy logs preserve their original ids; mode-3 logs include the durable lineage prefix.
pub fn message_mid(mode3: bool, lineage: Option<&str>, position: u64) -> Result<String, String> {
    if !mode3 {
        return Ok(format!("m{position}"));
    }
    let prefix = lineage_prefix(lineage)
        .ok_or_else(|| "mode-3 message id requires a durable lineage".to_owned())?;
    Ok(format!("m{prefix}.{position}"))
}

/// Decode only an id in this log's vocabulary. A different log's prefix is never a cursor.
pub fn position_of_mid(mid: &str, mode3: bool, lineage: Option<&str>) -> Option<u64> {
    let value = mid.strip_prefix('m')?;
    let digits = if mode3 {
        let (prefix, digits) = value.split_once('.')?;
        if lineage_prefix(lineage) != Some(prefix) {
            return None;
        }
        digits
    } else {
        value
    };
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_mid_spellings_keep_their_original_canonical_rules() {
        for (mid, position) in [
            ("m0", Some(0)),
            ("m7", Some(7)),
            ("m18446744073709551615", Some(u64::MAX)),
            ("m00", None),
            ("m07", None),
            ("m-1", None),
            ("m+1", None),
            ("m", None),
            ("m18446744073709551616", None),
            ("mabc01234.7", None),
        ] {
            assert_eq!(position_of_mid(mid, false, None), position, "{mid}");
        }
    }

    #[test]
    fn legacy_ids_survive_and_mode3_ids_fence_recreated_logs() {
        assert_eq!(message_mid(false, None, 0).unwrap(), "m0");
        let lineage = "abc0123487654321";
        assert_eq!(message_mid(true, Some(lineage), 7).unwrap(), "mabc01234.7");
        assert_eq!(position_of_mid("m0", false, None), Some(0));
        assert_eq!(position_of_mid("mabc01234.7", true, Some(lineage)), Some(7));
        assert_eq!(position_of_mid("mdef01234.7", true, Some(lineage)), None);
        assert_eq!(position_of_mid("m0", true, Some(lineage)), None);
        assert_eq!(position_of_mid("mabc01234.07", true, Some(lineage)), None);
        assert!(message_mid(true, None, 0).is_err());
        assert!(message_mid(true, Some("ABC0123487654321"), 0).is_err());
        assert_eq!(
            position_of_mid("mABC01234.7", true, Some("ABC0123487654321")),
            None
        );
        for position in [0, 1, 7, u64::MAX] {
            for mode3 in [false, true] {
                let mid = message_mid(mode3, Some(lineage), position).unwrap();
                assert_eq!(position_of_mid(&mid, mode3, Some(lineage)), Some(position));
            }
        }
    }
}
