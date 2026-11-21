//! Where a company AI gateway's key is sent, and how.

/// A company AI gateway's endpoint, as the vault serves it beside the gateway's key. The
/// operator sets these per credential when enrolling it. Only the key is secret; the rest shows
/// in the vault's own listings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayEndpoint {
    /// The API root that stands in for the provider's own (`https://api.anthropic.com`). The
    /// request path (`/v1/messages`) is appended to it, as to the provider's root.
    pub base_url: String,
    /// The header that carries the key.
    pub auth_header: String,
    /// How the key is written into that header.
    pub auth_scheme: GatewayScheme,
    /// Fixed headers sent with every request, in name order. The vault refuses a name that
    /// repeats the key's header, so the key is never sent twice.
    pub headers: Vec<(String, String)>,
}

/// How a gateway key is written into its header: `Bearer <key>` or the key alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayScheme {
    Bearer,
    Raw,
}

impl GatewayScheme {
    /// The vault's spelling (`bearer`, `raw`). Any other is not a gateway scheme.
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "bearer" => Some(Self::Bearer),
            "raw" => Some(Self::Raw),
            _ => None,
        }
    }
}

impl GatewayEndpoint {
    /// The headers a request to the gateway carries for `key`: the key's header first, then
    /// the fixed ones.
    pub fn auth_headers(&self, key: &str) -> Vec<(String, String)> {
        let value = match self.auth_scheme {
            GatewayScheme::Bearer => format!("Bearer {key}"),
            GatewayScheme::Raw => key.to_owned(),
        };
        std::iter::once((self.auth_header.clone(), value))
            .chain(self.headers.iter().cloned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(scheme: GatewayScheme) -> GatewayEndpoint {
        GatewayEndpoint {
            base_url: "https://gateway.example".into(),
            auth_header: "x-portkey-api-key".into(),
            auth_scheme: scheme,
            headers: vec![("x-portkey-provider".into(), "anthropic".into())],
        }
    }

    #[test]
    fn the_key_header_comes_first_written_by_its_scheme() {
        assert_eq!(
            endpoint(GatewayScheme::Bearer).auth_headers("k"),
            [
                ("x-portkey-api-key".to_string(), "Bearer k".to_string()),
                ("x-portkey-provider".to_string(), "anthropic".to_string()),
            ]
        );
        assert_eq!(
            endpoint(GatewayScheme::Raw).auth_headers("k")[0],
            ("x-portkey-api-key".to_string(), "k".to_string())
        );
    }

    #[test]
    fn only_the_vaults_two_schemes_parse() {
        assert_eq!(
            GatewayScheme::from_token("bearer"),
            Some(GatewayScheme::Bearer)
        );
        assert_eq!(GatewayScheme::from_token("raw"), Some(GatewayScheme::Raw));
        assert_eq!(GatewayScheme::from_token("basic"), None);
        assert_eq!(GatewayScheme::from_token("Bearer"), None);
    }
}
