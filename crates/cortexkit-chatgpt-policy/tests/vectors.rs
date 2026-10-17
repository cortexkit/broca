//! Run every shared golden vector under `test-vectors/` through the public API.

use cortexkit_chatgpt_policy::{account_id, classify_response, Classification, MissingAccountId};
use serde_json::Value;
use std::{
    path::Path,
    time::{Duration, UNIX_EPOCH},
};

fn vectors(file: &str) -> Vec<Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test-vectors")
        .join(file);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
    let document: Value = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("{} is not JSON: {error}", path.display()));
    document["vectors"]
        .as_array()
        .unwrap_or_else(|| panic!("{} has no vectors array", path.display()))
        .clone()
}

/// The vector-file spelling of a class, plus its reset fields when it has them.
fn describe(classification: Classification) -> Value {
    let class = match classification {
        Classification::AccountLimitExhausted {
            resets_at_ms,
            resets_at_source,
        } => {
            return serde_json::json!({
                "class": "account_limit_exhausted",
                "resets_at_ms": resets_at_ms,
                "resets_at_source": resets_at_source.map(|source| source.as_str()),
            })
        }
        Classification::UsageNotIncluded => "usage_not_included",
        Classification::AuthFailure => "auth_failure",
        Classification::UpstreamKeyRejection => "upstream_key_rejection",
        Classification::RateLimited => "rate_limited",
        Classification::Overloaded => "overloaded",
        Classification::Other => "other",
    };
    serde_json::json!({ "class": class })
}

#[test]
fn every_response_vector_gets_its_expected_class() {
    let vectors = vectors("responses.json");
    // The file holds at least one vector per class (two for some); requiring seven keeps an
    // emptied or truncated file from passing.
    assert!(
        vectors.len() >= 7,
        "only {} response vectors",
        vectors.len()
    );
    for vector in &vectors {
        let name = vector["name"].as_str().expect("vector name");
        let provenance = vector["provenance"].as_str().expect("vector provenance");
        assert!(
            provenance.starts_with("CAPTURED: ") || provenance.starts_with("RECONSTRUCTED: "),
            "{name}: provenance must say CAPTURED or RECONSTRUCTED"
        );
        let status = u16::try_from(vector["status"].as_u64().expect("status")).unwrap();
        let headers: Vec<(String, String)> = vector["headers"]
            .as_array()
            .expect("headers")
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().expect("header name").to_owned(),
                    pair[1].as_str().expect("header value").to_owned(),
                )
            })
            .collect();
        let body = vector["body"].as_str().expect("body");
        let now = UNIX_EPOCH + Duration::from_millis(vector["now_ms"].as_u64().expect("now_ms"));
        let actual = describe(classify_response(status, headers, body, now));
        assert_eq!(actual, vector["expected"], "{name}");
    }
}

#[test]
fn every_account_id_vector_resolves_as_expected() {
    let vectors = vectors("account_ids.json");
    assert!(
        vectors.len() >= 4,
        "only {} account-id vectors",
        vectors.len()
    );
    for vector in &vectors {
        let name = vector["name"].as_str().expect("vector name");
        let token = vector["access_token"].as_str().expect("access_token");
        let actual = account_id(vector["vault_account_id"].as_str(), token);
        let expected = &vector["expected"];
        match expected["account_id"].as_str() {
            Some(account) => assert_eq!(actual.as_deref(), Ok(account), "{name}"),
            None => {
                assert_eq!(expected["error"], "missing_account_id", "{name}");
                assert_eq!(actual, Err(MissingAccountId), "{name}");
            }
        }
    }
}
