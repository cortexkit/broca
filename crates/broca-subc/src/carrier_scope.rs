use std::{io::Read, str::FromStr};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use subc_protocol::{scope::ScopeSelector, Principal};

/// An owner's scope selector, without its privately supplied bind proof.
#[derive(Debug, Clone)]
pub struct CarrierScope {
    owner: String,
    reference: String,
    epoch: u64,
}

impl FromStr for CarrierScope {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = "invalid_scope: expected a reserved owner module id, scope ref and numeric epoch as owner/ref/epoch";
        let (owner, rest) = value.split_once('/').ok_or(invalid)?;
        let (reference, epoch) = rest.rsplit_once('/').ok_or(invalid)?;
        if owner.is_empty()
            || reference.is_empty()
            || owner.chars().any(|c| c.is_whitespace() || c.is_control())
            || reference.chars().any(char::is_control)
            || epoch.is_empty()
            || !epoch.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid);
        }
        Ok(Self {
            owner: owner.into(),
            reference: reference.into(),
            epoch: epoch.parse().map_err(|_| invalid)?,
        })
    }
}

impl CarrierScope {
    /// Read the owner's private hex proof without retaining or reporting its input bytes.
    pub fn read_proof(&self, input: &mut impl Read) -> Result<ScopeSelector, &'static str> {
        // Bound malformed input without ever including it in diagnostics. The owner closes
        // the private pipe after one proof, so EOF is part of the handoff protocol.
        let mut bytes = Vec::with_capacity(67);
        input
            .take(67)
            .read_to_end(&mut bytes)
            .map_err(|_| "scope_bind_proof_unreadable: could not read the private stdin proof")?;
        if bytes.is_empty() {
            return Err("scope_bind_proof_required: --scope requires a private 64-hex-character proof on stdin");
        }
        let bytes = bytes
            .strip_suffix(b"\r\n")
            .or_else(|| bytes.strip_suffix(b"\n"))
            .unwrap_or(&bytes);
        if bytes.len() != 64 || !bytes.iter().all(u8::is_ascii_hexdigit) {
            return Err("scope_bind_proof_invalid: expected exactly 64 hex characters on stdin");
        }
        let mut proof = [0u8; 32];
        for (index, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
            let digit = |byte: u8| (byte as char).to_digit(16).expect("validated ASCII hex") as u8;
            proof[index] = (digit(pair[0]) << 4) | digit(pair[1]);
        }
        Ok(ScopeSelector::new(
            Principal::Reserved {
                module_id: self.owner.clone(),
            },
            &self.reference,
            Some(self.epoch),
        )
        .with_bind_proof(Some(URL_SAFE_NO_PAD.encode(proof))))
    }
}

/// Refuse a Direct carrier when the daemon cannot enforce its scope and private proof.
/// Older daemons may otherwise ignore the selector and open an unscoped route.
pub async fn verify_direct_carrier_scope(
    conn: &crate::SubcConnection,
    selector: &ScopeSelector,
) -> Result<(), &'static str> {
    if conn.consumer_module_id().is_some() {
        return Err("scope_unsupported: a private carrier selector requires a Direct connection");
    }
    if selector.bind_proof.is_none() {
        return Err(
            "scope_bind_proof_required: a Direct carrier must supply its private bind proof",
        );
    }
    let capabilities = conn
        .server_capabilities()
        .await
        .map_err(|_| "scope_unverifiable: could not read the daemon's scope capabilities")?;
    if !capabilities
        .iter()
        .any(|cap| cap == subc_protocol::scope::CAP_SCOPES_V1)
        || !capabilities
            .iter()
            .any(|cap| cap == subc_protocol::manifest::CAP_SCOPE_BIND_PROOF_V1)
    {
        return Err("scope_unsupported: the daemon must advertise scopes/v1 and scope-bind-proof/v1 for a Direct carrier");
    }
    Ok(())
}

/// Keep a route refusal's code without exposing a diagnostic that may quote the private proof.
pub fn private_route_error(error: crate::ConnError) -> crate::ConnError {
    crate::ConnError::Rejected(crate::connection::RejectedMessage::structured(
        error.code().unwrap_or("scope_unverifiable").into(),
        "the Direct carrier route could not be opened".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_owner_record_uses_the_published_direct_carrier_shape() {
        let document = include_str!("../../../docs/broca-run.md");
        let record = document
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap()
            .replace("<sha256-of-the-raw-32-byte-proof>", &"0".repeat(64));
        let record: subc_protocol::scope::ScopeRecord = serde_json::from_str(&record).unwrap();
        assert_eq!(record.scope_ref, "run-42");
        assert_eq!(record.scope_epoch, 1);
        assert_eq!(record.workdir.as_deref(), Some("/absolute/rig-folder"));
        assert_eq!(record.carriers.len(), 1);
        assert_eq!(record.carriers[0].principal, Principal::Direct);
        assert_eq!(
            record.carriers[0].targets.as_deref(),
            Some(["aft".to_owned()].as_slice())
        );
        assert!(record.bind_proof_sha256.is_some());
    }

    #[test]
    fn parses_owner_ref_epoch_and_transcodes_private_hex_proof() {
        let scope: CarrierScope = "rig/nested/ref/7".parse().unwrap();
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let selector = scope.read_proof(&mut hex.as_bytes()).unwrap();
        assert_eq!(selector.scope_ref, "nested/ref");
        assert_eq!(selector.scope_epoch, Some(7));
        assert!(
            selector.bind_proof.as_deref() == Some("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
        );
        let debug = format!("{selector:?}");
        assert!(!debug.contains(hex));
        assert!(!debug.contains(selector.bind_proof.as_deref().unwrap()));
    }

    #[test]
    fn missing_and_malformed_proofs_have_named_secret_free_errors() {
        let scope: CarrierScope = "rig/run/1".parse().unwrap();
        assert!(scope
            .read_proof(&mut &b""[..])
            .unwrap_err()
            .starts_with("scope_bind_proof_required:"));
        for input in [
            "f".repeat(63),
            "f".repeat(65),
            "g".repeat(64),
            format!("{} ", "f".repeat(64)),
        ] {
            let error = scope.read_proof(&mut input.as_bytes()).unwrap_err();
            assert!(error.starts_with("scope_bind_proof_invalid:"));
            assert!(!error.contains(&input));
        }
    }

    #[test]
    fn malformed_scope_selectors_are_refused_by_name() {
        for value in [
            "rig",
            "rig/ref",
            "/ref/1",
            "rig//1",
            "rig/ref/x",
            "rig/ref/18446744073709551616",
            "rig\n/ref/1",
        ] {
            assert!(value
                .parse::<CarrierScope>()
                .unwrap_err()
                .starts_with("invalid_scope:"));
        }
    }
}
