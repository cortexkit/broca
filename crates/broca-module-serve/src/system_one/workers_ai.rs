//! Cloudflare Workers AI's decision models, Clef and Clef-flash.
//!
//! One item is one `POST {base}/accounts/{account_id}/ai/run/@cf/cloudflare/{model}` with
//! `authorization: Bearer <token>`, `base` being `https://api.cloudflare.com/client/v4`.
//! The body is the classifier role's provider request, `{model, state, questions, images?}`,
//! with `model` `"clef"` or `"clef-flash"`. Sources, read 2026-10-22:
//!
//! - the model pages, <https://developers.cloudflare.com/workers-ai/models/clef/> and
//!   <https://developers.cloudflare.com/workers-ai/models/clef-flash/>, with their input and
//!   output schemas (`schema-input.json`, `schema-output.json` beside each page): the request
//!   fields above, and the result `{model, answers, usage: {input_tokens, output_tokens}}`;
//! - the REST guide, <https://developers.cloudflare.com/workers-ai/get-started/rest-api/>:
//!   the bearer token, and the API's reply envelope `{result, success, errors, messages}`;
//! - the error reference, <https://developers.cloudflare.com/workers-ai/platform/errors/>:
//!   each error is an HTTP status with an internal code in `errors[].code` (3036 daily
//!   allocation used, 3040 out of capacity, 5007 no such model, 5035 paid plan, ...).
//!
//! A 200 whose envelope says `success: true` is answered with the `result` member's exact text,
//! so the engine reads the provider's answers and usage as Cloudflare spelled them. Any other
//! reply is a [`ProviderReply::WorkersAiError`] carrying the status and the first error code;
//! the engine maps it through the classifier role's Workers AI table.
//!
//! The token and the account id come from the vault credential
//! `apikey:cloudflare-workers-ai` on every call. The account id is part of the URL, so no
//! transport error text, URL or request detail leaves this module: an error is reduced to its
//! kind.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::credential::CredentialSource;
use super::{ClassifierProvider, ProviderReply};

/// The Cloudflare API base; a `providers.cloudflare-workers-ai.base_url` in broca's config
/// replaces it, as it does for a chat provider.
pub const DEFAULT_BASE_URL: &str = "https://api.cloudflare.com/client/v4";

/// The catalog provider id, which is also the key of its base-URL override.
pub const PROVIDER_ID: &str = "cloudflare-workers-ai";

/// The credential Workers AI calls present. The vault returns the account id with the token.
pub const CREDENTIAL_ID: &str = "apikey:cloudflare-workers-ai";

/// Clef's model path, as it follows `/ai/run/` in the URL.
pub const CLEF_MODEL_PATH: &str = "@cf/cloudflare/clef";
/// Clef-flash's model path.
pub const CLEF_FLASH_MODEL_PATH: &str = "@cf/cloudflare/clef-flash";

/// The account path segment used when there is no credential at all. That happens only on
/// broca's mock provider path, where no vault is opened and every request goes to a stand-in;
/// Cloudflare itself would refuse such a request for its missing token anyway.
pub const NO_CREDENTIAL_ACCOUNT: &str = "no-credential";

/// The Workers AI client for one model.
pub struct WorkersAi {
    http: reqwest::Client,
    base: url::Url,
    model_path: &'static str,
    credential: Arc<dyn CredentialSource>,
    attempt_timeout: Duration,
}

impl WorkersAi {
    /// A client for the model at `model_path` ([`CLEF_MODEL_PATH`] or
    /// [`CLEF_FLASH_MODEL_PATH`]), under `base_url` (`None` for [`DEFAULT_BASE_URL`]).
    pub fn new(
        base_url: Option<&str>,
        model_path: &'static str,
        credential: Arc<dyn CredentialSource>,
        attempt_timeout: Duration,
    ) -> Result<Self, String> {
        let base_text = base_url.unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/');
        let base = url::Url::parse(base_text)
            .map_err(|e| format!("Workers AI base URL {base_text:?}: {e}"))?;
        if base.host_str().is_none() || base.cannot_be_a_base() {
            return Err(format!("Workers AI base URL {base_text:?} names no host"));
        }
        Ok(Self {
            http: broca_provider::transport::build_provider_http_client()
                .without_response_decoding()
                .clone(),
            base,
            model_path,
            credential,
            attempt_timeout,
        })
    }

    /// The endpoint for `account_id`. It holds the account id, so it is never logged.
    pub fn endpoint_for(&self, account_id: &str) -> Result<url::Url, String> {
        if !is_account_id(account_id) {
            // The value itself is not shown: it is a credential field.
            return Err(
                "the Cloudflare account id the credential carries is not a plain id".to_owned(),
            );
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| "the Workers AI base URL cannot take a path".to_owned())?
            .pop_if_empty()
            .extend(["accounts", account_id, "ai", "run"])
            .extend(self.model_path.split('/'));
        Ok(url)
    }
}

/// A Cloudflare account id is 32 hex digits; anything outside letters, digits, `-` and `_`
/// could change the URL's path, so it is refused rather than escaped.
fn is_account_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// The parts of the Cloudflare API envelope this client reads. `result` is kept as its exact
/// text, so the answers' number spellings reach the caller unchanged.
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    result: Option<&'a RawValue>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    errors: Vec<EnvelopeError>,
}

#[derive(Deserialize)]
struct EnvelopeError {
    /// Cloudflare's internal error code. Read leniently: a code that is not a whole number
    /// is treated as absent, and the status decides.
    #[serde(default)]
    code: Option<serde_json::Value>,
}

/// The first error code in a reply body, when the body is the API envelope and names one.
fn first_error_code(body: &[u8]) -> Option<u64> {
    let envelope: Envelope<'_> = serde_json::from_slice(body).ok()?;
    envelope
        .errors
        .into_iter()
        .find_map(|error| error.code.as_ref().and_then(serde_json::Value::as_u64))
}

/// One attempt's reply, read: the `result` text of a successful 200, or the error.
pub(crate) fn reply_from(status: u16, retry_after_ms: Option<u64>, body: &[u8]) -> ProviderReply {
    if status == 200 {
        if let Ok(envelope) = serde_json::from_slice::<Envelope<'_>>(body) {
            let result = envelope
                .result
                .filter(|result| result.get().trim_start().starts_with('{'));
            if let (Some(true), Some(result)) = (envelope.success, result) {
                return ProviderReply::Answered(result.get().as_bytes().to_vec());
            }
        }
        // A 200 that is not a successful envelope carries no answer. The status alone maps
        // to nothing, so the engine records it as a provider error naming any code it gave.
        return ProviderReply::WorkersAiError {
            status,
            code: first_error_code(body),
            retry_after_ms: None,
        };
    }
    ProviderReply::WorkersAiError {
        status,
        code: first_error_code(body),
        retry_after_ms,
    }
}

#[async_trait]
impl ClassifierProvider for WorkersAi {
    fn egress_host(&self) -> String {
        self.base.host_str().unwrap_or_default().to_owned()
    }

    fn credential_id(&self) -> Option<String> {
        self.credential.credential_id()
    }

    async fn call(&self, body: String) -> ProviderReply {
        let secret = match self.credential.secret().await {
            Ok(secret) => secret,
            Err(message) => return ProviderReply::Credential(message),
        };
        let auth_selection = secret.as_ref().and_then(|s| s.auth_selection.clone());
        let account_ref = secret.as_ref().and_then(|s| s.account_id.clone());
        let (token, account_id) = match secret {
            Some(secret) => match secret.account_id {
                Some(account_id) => (Some(secret.token), account_id),
                None => {
                    return ProviderReply::Credential(format!(
                        "the credential {} carries no Cloudflare account id",
                        self.credential
                            .credential_id()
                            .unwrap_or_else(|| CREDENTIAL_ID.to_owned())
                    ))
                }
            },
            None => (None, NO_CREDENTIAL_ACCOUNT.to_owned()),
        };
        let endpoint = match self.endpoint_for(&account_id) {
            Ok(endpoint) => endpoint,
            Err(message) => return ProviderReply::Credential(message),
        };
        let mut request = self
            .http
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let attempt = async {
            let response = request.send().await?;
            let status = response.status().as_u16();
            let retry_after = super::typesafe::retry_after_ms(response.headers());
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, retry_after, body))
        };
        match tokio::time::timeout(self.attempt_timeout, attempt).await {
            Err(_) => ProviderReply::Transport(format!(
                "no reply within {} ms",
                self.attempt_timeout.as_millis()
            )),
            // reqwest's error text names the URL, which holds the account id: only the kind of
            // failure is kept.
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
            Ok(Ok((status, retry_after_ms, body))) => {
                match reply_from(status, retry_after_ms, &body) {
                    ProviderReply::Answered(body) => ProviderReply::AttributedAnswer {
                        body,
                        auth_selection,
                        account_ref,
                    },
                    reply => reply,
                }
            }
        }
    }
}
