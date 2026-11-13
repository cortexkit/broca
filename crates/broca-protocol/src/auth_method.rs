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
}

impl AuthMethod {
    /// Every method, in declaration order.
    pub const ALL: [Self; 4] = [Self::ApiKey, Self::Oauth, Self::Antigravity, Self::Chatgpt];

    /// The stable credential-id token for this method.
    pub fn as_token(self) -> &'static str {
        match self {
            Self::ApiKey => "apikey",
            Self::Oauth => "oauth",
            Self::Antigravity => "antigravity",
            Self::Chatgpt => "chatgpt",
        }
    }

    /// Parse the first credential-id segment.
    pub fn from_token(token: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|method| method.as_token() == token)
    }

    /// Whether the method signs in to a subscription account, whose use draws on the account's
    /// quota rather than being billed per token. Every method but an API key does.
    pub fn is_subscription(self) -> bool {
        match self {
            Self::ApiKey => false,
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
    fn only_an_api_key_is_metered() {
        let metered: Vec<_> = AuthMethod::ALL
            .into_iter()
            .filter(|method| !method.is_subscription())
            .collect();
        assert_eq!(metered, [AuthMethod::ApiKey]);
    }
}
