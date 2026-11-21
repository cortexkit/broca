//! How a provider authenticates for a run.

/// How a provider authenticates for a run. The stable token is also the first segment of the
/// credential id used by the vault-backed module.
///
/// Every method takes its account from routing at each run start. Code that treats methods
/// differently matches this enum exhaustively, so a new method fails to compile until each
/// site decides what it means for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// A static API key. Anthropic uses this method to select API-key-specific body policies.
    ApiKey,
    /// Provider OAuth, including Anthropic's Claude-subscription request shape.
    Oauth,
    /// Google Antigravity / Code-Assist OAuth.
    Antigravity,
    /// OpenAI ChatGPT-subscription OAuth.
    Chatgpt,
    /// A company AI gateway's key. The request goes to the gateway's own address, which the
    /// vault holds with the key, and the gateway pays the provider.
    Gateway,
}

impl AuthMethod {
    /// Every method, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::ApiKey,
        Self::Oauth,
        Self::Antigravity,
        Self::Chatgpt,
        Self::Gateway,
    ];

    /// The stable credential-id token for this method.
    pub fn as_token(self) -> &'static str {
        match self {
            Self::ApiKey => "apikey",
            Self::Oauth => "oauth",
            Self::Antigravity => "antigravity",
            Self::Chatgpt => "chatgpt",
            Self::Gateway => "gateway",
        }
    }

    /// Parse the first credential-id segment.
    pub fn from_token(token: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|method| method.as_token() == token)
    }

    /// Whether the method signs in to a subscription account, whose use draws on the account's
    /// quota rather than being billed per token. An API key and a gateway key are billed per
    /// token; every other method signs in to a subscription.
    pub fn is_subscription(self) -> bool {
        match self {
            Self::ApiKey | Self::Gateway => false,
            Self::Oauth | Self::Antigravity | Self::Chatgpt => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AuthMethod;

    #[test]
    fn tokens_round_trip_and_unknown_tokens_are_refused() {
        for method in AuthMethod::ALL {
            assert_eq!(AuthMethod::from_token(method.as_token()), Some(method));
        }
        assert_eq!(AuthMethod::from_token("cursor"), None);
        assert_eq!(AuthMethod::from_token(""), None);
    }

    #[test]
    fn only_api_and_gateway_keys_are_metered() {
        let metered: Vec<_> = AuthMethod::ALL
            .into_iter()
            .filter(|method| !method.is_subscription())
            .collect();
        assert_eq!(metered, [AuthMethod::ApiKey, AuthMethod::Gateway]);
    }
}
