//! One codec owns opaque reference IDs and their history-snapshot scope.

use super::SetKey;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

pub(super) fn snapshot_scope(incarnation: &str, history_version: u64) -> String {
    format!(
        "h1.{}.{}",
        URL_SAFE_NO_PAD.encode(incarnation.as_bytes()),
        history_version
    )
}

fn canonical(text: &str) -> Option<u64> {
    let number: u64 = text.parse().ok()?;
    (number.to_string() == text).then_some(number)
}

struct ReferenceId<'a> {
    scope: &'a str,
    key: SetKey,
    record: Option<usize>,
}

impl<'a> ReferenceId<'a> {
    fn parse(id: &'a str) -> Option<Self> {
        let (scope, rest) = id.split_once("/m")?;
        let (encoded, version) = scope.strip_prefix("h1.")?.split_once('.')?;
        canonical(version)?;
        let incarnation = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        if incarnation.is_empty()
            || incarnation.len() > 128
            || std::str::from_utf8(&incarnation).is_err()
            || URL_SAFE_NO_PAD.encode(&incarnation) != encoded
        {
            return None;
        }
        let (set, record) = match rest.split_once("/r") {
            Some((set, record)) => (set, Some(usize::try_from(canonical(record)?).ok()?)),
            None => (rest, None),
        };
        let (ordinal, call) = match set.split_once("/c") {
            Some((ordinal, call)) => (ordinal, Some(usize::try_from(canonical(call)?).ok()?)),
            None => (set, None),
        };
        Some(Self {
            scope,
            key: SetKey {
                ordinal: canonical(ordinal)?,
                call,
            },
            record,
        })
    }

    fn encode(&self) -> String {
        let mut id = format!("{}/m{}", self.scope, self.key.ordinal);
        if let Some(call) = self.key.call {
            id.push_str(&format!("/c{call}"));
        }
        if let Some(record) = self.record {
            id.push_str(&format!("/r{record}"));
        }
        id
    }
}

pub(super) fn set_id(scope: &str, key: SetKey) -> String {
    ReferenceId {
        scope,
        key,
        record: None,
    }
    .encode()
}

pub(super) fn ref_id(set: &str, index: usize) -> String {
    let mut id = ReferenceId::parse(set).expect("reference ids extend only locally minted set ids");
    id.record = Some(index);
    id.encode()
}

pub(super) fn parse_set(id: &str) -> Option<(&str, SetKey)> {
    let id = ReferenceId::parse(id)?;
    id.record.is_none().then_some((id.scope, id.key))
}

pub(super) fn parse_ref(id: &str) -> Option<(&str, SetKey, usize)> {
    let id = ReferenceId::parse(id)?;
    Some((id.scope, id.key, id.record?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_codec_round_trips_snapshot_scoped_set_and_record_ids() {
        let scope = snapshot_scope("lineage/with.delimiters", 123);
        let key = SetKey {
            ordinal: 7,
            call: Some(2),
        };
        let set = set_id(&scope, key);
        assert_eq!(parse_set(&set), Some((scope.as_str(), key)));
        let record = ref_id(&set, 3);
        assert_eq!(parse_ref(&record), Some((scope.as_str(), key, 3)));
        assert!(parse_set(&record).is_none());
        assert!(parse_ref(&set).is_none());
        assert!(parse_set("lineage/m7/c2").is_none());
        assert!(parse_ref("lineage/m7/c2/r3").is_none());
        assert_ne!(
            snapshot_scope("lineage", 123),
            snapshot_scope("lineage", 124)
        );
    }
}
