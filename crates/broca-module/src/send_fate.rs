//! Two rules a send's status is read by: the run id a retrying sender's send names, and where a
//! delivered send's call key comes from.

use broca_protocol::{BindPrincipal, RunId};

pub(crate) fn qualified_sender_run_id(
    base: &RunId,
    sender: &BindPrincipal,
) -> Result<RunId, String> {
    use sha2::Digest;
    let module_id = match sender {
        BindPrincipal::Reserved { module_id } => module_id,
        BindPrincipal::Direct | BindPrincipal::Unverified | BindPrincipal::Unknown(_) => {
            return Err("a sender-qualified ID requires a reserved module principal".into());
        }
    };
    // Retries recompute this ID across upgrades. Hash only the fixed version/kind tags
    // and the module_id's UTF-8 bytes, never serde's evolving principal representation.
    let mut digest = sha2::Sha256::new();
    digest.update(b"v1\0reserved\0");
    digest.update(module_id.as_bytes());
    Ok(RunId(format!("{}-sender-{:x}", base.0, digest.finalize())))
}

/// Where the call key a delivered send's `delivered` names comes from.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DeliveredCallKey {
    /// The key the delivery's ride envelope recorded.
    Ride(String),
    /// A send delivered with a tool result before envelopes existed: the key of the call whose
    /// result carried it.
    Carrier,
    /// The send names no call.
    Nothing,
}

/// The rule a delivered send's call key is read by: the ride envelope's key when
/// the delivery has one (`ride_key`), else, only for a send delivered with a tool result, the
/// carrier call's key.
pub(crate) fn delivered_call_key(
    ride_key: Option<&str>,
    mode: broca_protocol::DeliveryMode,
) -> DeliveredCallKey {
    match ride_key {
        Some(key) => DeliveredCallKey::Ride(key.to_owned()),
        None if mode == broca_protocol::DeliveryMode::ToolResult => DeliveredCallKey::Carrier,
        None => DeliveredCallKey::Nothing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_sender_run_id_encoding_is_pinned() {
        assert_eq!(
            qualified_sender_run_id(
                &RunId("run-sid-shared".into()),
                &BindPrincipal::Reserved { module_id: "ck-mc".into() },
            ).unwrap(),
            RunId("run-sid-shared-sender-efbcae06a66482a915f9cd18ad50e0e8d67996b2710ef552318a00b1d9906dff".into()),
        );
    }

    #[test]
    fn qualified_sender_run_id_refuses_unattested_principals() {
        for principal in [
            BindPrincipal::Direct,
            BindPrincipal::Unverified,
            BindPrincipal::Unknown(serde_json::json!({"kind":"future","module_id":"ck-mc"})),
        ] {
            assert!(qualified_sender_run_id(&RunId("run-sid-shared".into()), &principal).is_err());
        }
    }
}
