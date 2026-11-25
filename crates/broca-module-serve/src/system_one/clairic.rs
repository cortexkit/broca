//! Clairic's provider replies over the shared Workers AI connection.

use std::time::Duration;

use cortexkit_role_extractor::{
    errors::{self, ItemError},
    provider::{self, ProviderResult},
};

use super::{
    workers_ai::{HttpResponse, WorkersAi},
    ProviderReply, SystemOne,
};

pub(super) struct Answered {
    pub result: ProviderResult,
    pub auth_selection: Option<String>,
    pub account_ref: Option<String>,
}

pub(super) struct Failed {
    pub error: ItemError,
    pub may_have_spent: bool,
    retry: Option<Duration>,
}

fn response(reply: HttpResponse<'_>) -> Result<Answered, Box<Failed>> {
    let text = std::str::from_utf8(reply.body).unwrap_or_default();
    match provider::read_reply(reply.status, text) {
        Ok(result) => Ok(Answered {
            result,
            auth_selection: reply.auth_selection,
            account_ref: reply.account_ref,
        }),
        Err(mut error) => {
            if let Some(wait) = reply.retry_after_ms {
                error = error.with_retry_after_ms(wait);
            }
            let retry = reply
                .retry_after_ms
                .filter(|_| matches!(reply.status, 429 | 503) && error.code == errors::RATE_LIMITED)
                .map(Duration::from_millis);
            // An overload is rejected before inference. Other provider errors, including
            // missing or malformed successful replies, may have reached a billed model.
            let may_have_spent = error.code == errors::PROVIDER_ERROR && reply.status != 503;
            Err(Box::new(Failed {
                error,
                may_have_spent,
                retry,
            }))
        }
    }
}

fn transport_failure(reply: ProviderReply) -> Box<Failed> {
    let (code, message, may_have_spent) = match reply {
        ProviderReply::Credential(message) => (errors::AUTH_FAILED, message, false),
        ProviderReply::CredentialUnavailable(message) | ProviderReply::Unconnected(message) => {
            (errors::PROVIDER_ERROR, message, false)
        }
        ProviderReply::Transport(message) => (errors::PROVIDER_ERROR, message, true),
        _ => (
            errors::PROVIDER_ERROR,
            "Workers AI returned no HTTP response".into(),
            true,
        ),
    };
    Box::new(Failed {
        error: ItemError::for_code(code, message),
        may_have_spent,
        retry: None,
    })
}

pub(super) async fn call(
    service: &SystemOne,
    provider: &WorkersAi,
    model: &str,
    body: &str,
) -> Result<Answered, Box<Failed>> {
    let slot = service.model_slot(model);
    for attempt in 1..=service.config.max_attempts.max(1) {
        let result = {
            let _permit = slot.acquire().await;
            provider
                .send(body.to_owned(), response)
                .await
                .unwrap_or_else(|failure| Err(transport_failure(*failure)))
        };
        if let Err(failed) = &result {
            service
                .metrics
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_provider_error
                .insert(
                    super::workers_ai::PROVIDER_ID.into(),
                    failed.error.code.clone(),
                );
            if let Some(wait) = failed.retry.filter(|wait| {
                attempt < service.config.max_attempts && *wait <= service.config.max_retry_wait
            }) {
                tokio::time::sleep(wait).await;
                continue;
            }
        }
        return result;
    }
    unreachable!("the last provider attempt cannot retry")
}

#[cfg(test)]
#[test]
fn all_captured_workers_ai_envelopes_map_to_the_pinned_item_outcomes() {
    use cortexkit_role_extractor::{reply::ItemResult, vectors::PROVIDER_REPLIES_JSON};

    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        status: u16,
        body: String,
        item: ItemResult,
    }
    #[derive(serde::Deserialize)]
    struct Captures {
        replies: Vec<Case>,
    }
    let captures: Captures = serde_json::from_str(PROVIDER_REPLIES_JSON).unwrap();
    assert_eq!(captures.replies.len(), 22);
    for capture in captures.replies {
        let answer = match response(HttpResponse {
            status: capture.status,
            retry_after_ms: None,
            body: capture.body.as_bytes(),
            auth_selection: None,
            account_ref: None,
        }) {
            Ok(answer) => answer.result.into_item(0),
            Err(failure) => ItemResult::failed(0, failure.error),
        };
        assert_eq!(
            serde_json::to_string(&answer).unwrap(),
            serde_json::to_string(&capture.item).unwrap(),
            "{}",
            capture.name,
        );
    }
}
