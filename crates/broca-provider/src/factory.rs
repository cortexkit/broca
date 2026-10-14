//! Runtime construction of a live `Provider` from a frozen render config (feature
//! `live`).
//!
//! The catalog resolves a provider to a `FrozenRenderConfig` carrying a RUNTIME
//! `WireFamilyId`, but [`LiveProvider`] is generic over a CONCRETE family type. This
//! module bridges the two: it matches on `frozen.family_id` and constructs the right
//! `LiveProvider<ConcreteFamily>` boxed as `dyn Provider`.
//!
//! The match is EXHAUSTIVE over the closed family enum on purpose — adding a sixth
//! family is a compile error here until it is wired, rather than a silent runtime
//! fallthrough.

use std::sync::Arc;

use broca_core::provider::Provider;

use crate::credential_refresh::CredentialRefresher;

use crate::families::{
    AnthropicMessages, BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses,
    OpenAiResponsesChatgpt,
};
use crate::spec::WireFamilyId;
use crate::transport::{LiveProvider, ProviderHttpClient};
use crate::wire::{AuthHandle, FrozenRenderConfig, WireFamily};

/// A frozen config named a wire family that has no live transport wiring. With a
/// closed, fully-wired family enum this is currently unreachable, but it keeps the
/// constructor total rather than panicking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedFamily(pub WireFamilyId);

impl std::fmt::Display for UnsupportedFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no live transport is wired for wire family {:?}", self.0)
    }
}

impl std::error::Error for UnsupportedFamily {}

/// Build a live `Provider` for a frozen render config + a runtime auth credential.
///
/// The credential is supplied by the caller (resolved from the provider's `AuthKind`)
/// and attached to the request AFTER the body is rendered, so it never enters the
/// cache/resume hash. The returned provider renders exactly the bytes the frozen
/// config pins — identical on a fresh run and on resume.
pub fn live_provider(
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
) -> Result<Box<dyn Provider>, UnsupportedFamily> {
    live_provider_inner(
        None,
        frozen,
        auth,
        None,
        None,
        None,
        broca_core::clock::Clock::default(),
    )
}

/// Build a live provider with optional in-step credential refresh and the caller's shared clock.
pub fn live_provider_with_clock_and_refresher(
    client: Option<ProviderHttpClient>,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    refresher: Option<Arc<dyn CredentialRefresher>>,
    frozen_account_ref: Option<String>,
    served_account_ref: Option<String>,
    clock: broca_core::clock::Clock,
) -> Result<Box<dyn Provider>, UnsupportedFamily> {
    live_provider_inner(
        client,
        frozen,
        auth,
        refresher,
        frozen_account_ref,
        served_account_ref,
        clock,
    )
}

struct ProviderOptions {
    clock: broca_core::clock::Clock,
    refresher: Option<Arc<dyn CredentialRefresher>>,
    frozen_account_ref: Option<String>,
    served_account_ref: Option<String>,
}

fn live_provider_inner(
    client: Option<ProviderHttpClient>,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    refresher: Option<Arc<dyn CredentialRefresher>>,
    frozen_account_ref: Option<String>,
    served_account_ref: Option<String>,
    clock: broca_core::clock::Clock,
) -> Result<Box<dyn Provider>, UnsupportedFamily> {
    let options = ProviderOptions {
        clock,
        refresher,
        frozen_account_ref,
        served_account_ref,
    };
    Ok(match frozen.family_id {
        WireFamilyId::AnthropicMessages => {
            boxed_provider(client, AnthropicMessages, frozen, auth, options)
        }
        WireFamilyId::OpenAiChat => boxed_provider(client, OpenAiChat, frozen, auth, options),
        WireFamilyId::OpenAiResponses => {
            boxed_provider(client, OpenAiResponses, frozen, auth, options)
        }
        WireFamilyId::Gemini => boxed_provider(client, Gemini, frozen, auth, options),
        WireFamilyId::BedrockConverse => {
            boxed_provider(client, BedrockConverse, frozen, auth, options)
        }
        WireFamilyId::GoogleCodeAssist => {
            boxed_provider(client, GoogleCodeAssist, frozen, auth, options)
        }
        WireFamilyId::OpenAiResponsesChatgpt => {
            boxed_provider(client, OpenAiResponsesChatgpt, frozen, auth, options)
        }
    })
}

fn boxed_provider<F: WireFamily + 'static>(
    client: Option<ProviderHttpClient>,
    family: F,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    options: ProviderOptions,
) -> Box<dyn Provider> {
    let ProviderOptions {
        clock,
        refresher,
        frozen_account_ref,
        served_account_ref,
    } = options;
    let provider = match client {
        Some(client) => LiveProvider::with_client_and_clock(client, family, frozen, auth, clock),
        None => LiveProvider::new_with_clock(family, frozen, auth, clock),
    };
    let provider = provider.with_account_refs(frozen_account_ref, served_account_ref);
    match refresher {
        Some(refresher) => Box::new(provider.with_credential_refresher(refresher)),
        None => Box::new(provider),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::{frozen_config_for_family, renderer_version_for};

    #[test]
    fn builds_a_provider_for_each_family_id() {
        // Every family id constructs a boxed provider (the exhaustive match has an
        // arm for each), and the boxed provider renders from the frozen config.
        for family in [
            WireFamilyId::AnthropicMessages,
            WireFamilyId::OpenAiChat,
            WireFamilyId::OpenAiResponses,
            WireFamilyId::Gemini,
            WireFamilyId::BedrockConverse,
            WireFamilyId::GoogleCodeAssist,
            WireFamilyId::OpenAiResponsesChatgpt,
        ] {
            let frozen = frozen_config_for_family(family, "m");
            let provider = live_provider(frozen, AuthHandle::None).expect("every family is wired");
            // The renderer version comes straight off the frozen config.
            assert_eq!(provider.renderer_version(), renderer_version_for(family));
        }
    }
}
