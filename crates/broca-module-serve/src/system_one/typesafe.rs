//! TypeSafe's classification endpoint (Jev), `POST {base}/systemone` with a bearer token.
//!
//! TypeSafe documents no error body, so a refusal is read by its HTTP status alone, plus the
//! `Retry-After` it may name; the mapping to item codes is the classifier role's (by status).
//! The token comes from the credential vault each call, and is never logged or returned.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::{ClassifierProvider, ProviderReply};

// The credential types lived here before a second provider shared them.
pub use super::credential::{CredentialSource, NoCredential, ProviderSecret, VaultCredential};

/// TypeSafe's API base; the endpoint is this plus `/systemone`. A `providers.typesafe.base_url`
/// in broca's config replaces it, as it does for a chat provider.
pub const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai/v1";

/// The credential TypeSafe calls present.
pub const CREDENTIAL_ID: &str = "apikey:typesafe";

/// The TypeSafe client.
pub struct TypeSafe {
    http: reqwest::Client,
    endpoint: url::Url,
    credential: Arc<dyn CredentialSource>,
    attempt_timeout: Duration,
}

impl TypeSafe {
    /// A client for `base_url` (`None` for [`DEFAULT_BASE_URL`]).
    pub fn new(
        base_url: Option<&str>,
        credential: Arc<dyn CredentialSource>,
        attempt_timeout: Duration,
    ) -> Result<Self, String> {
        let base = base_url.unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/');
        let endpoint = url::Url::parse(&format!("{base}/systemone"))
            .map_err(|e| format!("TypeSafe base URL {base:?}: {e}"))?;
        if endpoint.host_str().is_none() {
            return Err(format!("TypeSafe base URL {base:?} names no host"));
        }
        Ok(Self {
            http: broca_provider::transport::build_provider_http_client()
                .without_response_decoding()
                .clone(),
            endpoint,
            credential,
            attempt_timeout,
        })
    }

    pub fn endpoint(&self) -> &url::Url {
        &self.endpoint
    }
}

/// `Retry-After` in whole seconds, as milliseconds. An HTTP-date form is not read: the
/// provider documents none, and an unread wait leaves the error to the caller.
pub(crate) fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let seconds: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    seconds.checked_mul(1000)
}

#[async_trait]
impl ClassifierProvider for TypeSafe {
    fn egress_host(&self) -> String {
        self.endpoint.host_str().unwrap_or_default().to_owned()
    }

    fn credential_id(&self) -> Option<String> {
        self.credential.credential_id()
    }

    async fn call(&self, body: String) -> ProviderReply {
        let secret = match self.credential.secret().await {
            Ok(secret) => secret,
            Err(message) => return ProviderReply::Credential(message),
        };
        let mut request = self
            .http
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(secret) = &secret {
            request = request.bearer_auth(&secret.token);
        }
        let attempt = async {
            let response = request.send().await?;
            let status = response.status().as_u16();
            let retry_after = retry_after_ms(response.headers());
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, retry_after, body))
        };
        match tokio::time::timeout(self.attempt_timeout, attempt).await {
            Err(_) => ProviderReply::Transport(format!(
                "no reply within {} ms",
                self.attempt_timeout.as_millis()
            )),
            // The URL is the public endpoint and carries no secret, but the error text is
            // reduced to its kind anyway so nothing request-specific reaches a reply.
            // A connect error comes from the connector, before any request byte is written.
            // A connect that timed out stays a transport failure, like every timeout.
            Ok(Err(error)) if error.is_connect() && !error.is_timeout() => {
                ProviderReply::Unconnected("the connection failed".to_owned())
            }
            Ok(Err(error)) => ProviderReply::Transport(if error.is_timeout() {
                "the request timed out".to_owned()
            } else {
                "the request failed in transport".to_owned()
            }),
            Ok(Ok((200, _, body))) => ProviderReply::AttributedAnswer {
                body: body.to_vec(),
                auth_selection: secret.as_ref().and_then(|s| s.auth_selection.clone()),
                account_ref: secret.and_then(|s| s.account_id),
            },
            Ok(Ok((status, retry_after_ms, _))) => ProviderReply::Status {
                status,
                retry_after_ms,
            },
        }
    }
}
