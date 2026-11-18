//! Where a credential may be sent.
//!
//! A request's URL comes from the model catalog, and a catalog row can be wrong or forged. A
//! credential's vault record lists the hosts that credential is for, so every request that
//! carries one is checked against that list before it is sent, and nothing the catalog serves
//! can widen it. A record that lists no hosts admits none: there is no fallback to the catalog.

use std::sync::Arc;

use broca_protocol::{ErrorClass, ProviderError};

use crate::wire::AuthHandle;

/// The `provider_code` of a request refused because its credential's record doesn't list the
/// request's host.
pub const CREDENTIAL_HOST_NOT_ALLOWED: &str = "credential_host_not_allowed";

/// The hosts a credential's vault record lists, as exact lowercase host names (no scheme, port,
/// path or wildcard). Empty when the record lists none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialHosts(Arc<[String]>);

/// The local machine only: what a provider built without a vault record may reach, such as a
/// stand-in server in a test. Every credential read from the vault replaces it with its
/// record's own list, so a real credential is never sent anywhere this default allows unless
/// its record lists that host too.
impl Default for CredentialHosts {
    fn default() -> Self {
        Self(
            ["127.0.0.1", "localhost", "[::1]"]
                .map(str::to_owned)
                .into(),
        )
    }
}

impl CredentialHosts {
    /// The hosts a record lists, lowercased. `None` (the record has no host fact) admits nothing.
    pub fn from_record(hosts: Option<&[String]>) -> Self {
        Self(
            hosts
                .unwrap_or_default()
                .iter()
                .map(|host| host.to_ascii_lowercase())
                .collect(),
        )
    }

    /// Refuses `url` unless `auth` carries no credential or the credential's record lists the
    /// URL's host exactly. The host is compared lowercased and without its port, so a
    /// subdomain is admitted only when it is listed itself.
    pub fn check(&self, auth: &AuthHandle, url: &str) -> Result<(), Box<ProviderError>> {
        if matches!(auth, AuthHandle::None) {
            return Ok(());
        }
        self.check_url(url)
    }

    /// Refuses `url` unless the credential's record lists its host exactly, for a caller that
    /// is about to attach the credential to a request for `url`.
    pub fn check_url(&self, url: &str) -> Result<(), Box<ProviderError>> {
        let host = reqwest::Url::parse(url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_ascii_lowercase));
        if let Some(host) = &host {
            if self.0.iter().any(|listed| listed == host) {
                return Ok(());
            }
            if cfg!(feature = "test-loopback-credentials")
                && Self::default().0.iter().any(|local| local == host)
            {
                return Ok(());
            }
        }
        let message = match (&host, self.0.is_empty()) {
            (None, _) => "the request URL names no host; its credential was not sent".to_owned(),
            (Some(host), true) => format!(
                "the credential's vault record lists no hosts, so it was not sent to '{host}'"
            ),
            (Some(host), false) => format!(
                "the credential's vault record does not list host '{host}', so it was not sent"
            ),
        };
        Err(Box::new(host_refused(message)))
    }
}

fn host_refused(message: String) -> ProviderError {
    ProviderError {
        class: ErrorClass::Permanent,
        message,
        provider_code: Some(CREDENTIAL_HOST_NOT_ALLOWED.into()),
        status: None,
        retry_after_secs: None,
        provider_subcode: None,
        provider_details: None,
        cause: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed(hosts: &[&str]) -> CredentialHosts {
        let hosts: Vec<String> = hosts.iter().map(|host| (*host).to_owned()).collect();
        CredentialHosts::from_record(Some(&hosts))
    }

    fn key() -> AuthHandle {
        AuthHandle::Bearer("secret".into())
    }

    #[test]
    fn a_listed_host_is_admitted_whatever_its_case_port_or_path() {
        let hosts = listed(&["API.anthropic.com"]);
        for url in [
            "https://api.anthropic.com/v1/messages",
            "https://API.Anthropic.com:443/v1/messages?beta=true",
            "wss://api.anthropic.com/socket",
        ] {
            assert!(hosts.check(&key(), url).is_ok(), "{url}");
        }
    }

    #[test]
    fn an_unlisted_host_a_subdomain_or_an_absent_list_is_refused() {
        let hosts = listed(&["anthropic.com"]);
        for url in [
            "https://api.anthropic.com/v1/messages",
            "https://anthropic.com.evil.example/v1/messages",
            "not a url",
        ] {
            let error = hosts.check(&key(), url).unwrap_err();
            assert_eq!(error.class, ErrorClass::Permanent, "{url}");
            assert_eq!(
                error.provider_code.as_deref(),
                Some(CREDENTIAL_HOST_NOT_ALLOWED)
            );
        }
        let error = CredentialHosts::from_record(None)
            .check(&key(), "https://api.anthropic.com/v1/messages")
            .unwrap_err();
        assert!(
            error.message.contains("lists no hosts"),
            "{}",
            error.message
        );
        assert!(CredentialHosts::from_record(None)
            .check(&key(), "http://127.0.0.1:4010/v1/messages")
            .is_err());
    }

    #[test]
    fn the_default_reaches_only_the_local_machine() {
        let local = CredentialHosts::default();
        for url in [
            "http://127.0.0.1:1/x",
            "http://localhost:1/x",
            "http://[::1]:1/x",
        ] {
            assert!(local.check(&key(), url).is_ok(), "{url}");
        }
        assert!(local
            .check(&key(), "https://api.anthropic.com/v1/messages")
            .is_err());
    }

    #[test]
    fn a_request_without_a_credential_is_never_refused() {
        assert!(CredentialHosts::from_record(None)
            .check(&AuthHandle::None, "http://127.0.0.1:4010/v1/messages")
            .is_ok());
    }

    #[test]
    fn the_refusal_never_carries_the_credential() {
        let error = listed(&["api.openai.com"])
            .check(&key(), "https://elsewhere.example/v1")
            .unwrap_err();
        assert!(!format!("{error:?}").contains("secret"));
    }
}
