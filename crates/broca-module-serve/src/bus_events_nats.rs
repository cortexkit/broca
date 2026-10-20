//! ck-bus JWT custody and the real JetStream claim-check adapter.

use crate::bus_events::{now_ms, BusConnection, BusConnector};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::BindIdentity;
use broca_store::bus_events::{BusPublisher, Publication};
use broca_subc::{connection::RouteOpenRetry, SubcConnection};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection, NatsStream};
use cortexkit_bus_trait::{PublishReceipt, Stream};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

// Credential bodies deliberately have no Debug implementation and are never logged.
#[derive(Clone, Deserialize)]
struct Credential {
    jwt: String,
    exp: u64,
    acct: String,
    inbox_prefix: String,
    server_url: String,
    credential_public: String,
}

struct Credentials {
    conn: Arc<SubcConnection>,
    bind: BindIdentity,
}
#[async_trait]
trait CredentialRoute: Send + Sync {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String>;
}
impl Credentials {
    fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            conn,
            bind: BindIdentity::new("/", "broca", "bus-events"),
        }
    }
}
#[async_trait]
impl CredentialRoute for Credentials {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let route = self
            .conn
            .route_open_management_within(
                "ckbus",
                &self.bind,
                RouteOpenRetry {
                    budget: Duration::from_secs(5),
                    max_interval: Duration::from_millis(250),
                },
            )
            .await
            .map_err(|_| "ckbus route unavailable".to_string())?;
        let answer = tokio::time::timeout(
            Duration::from_secs(5),
            self.conn.route_request(
                &route,
                serde_json::to_vec(&serde_json::json!({"method":method,"params":params}))
                    .map_err(|_| "encode ckbus request")?,
            ),
        )
        .await;
        let _ = self.conn.route_close(&route).await;
        let value = answer
            .map_err(|_| "ckbus request timed out")?
            .map_err(|_| "ckbus request refused or unavailable")?;
        value
            .get("result")
            .cloned()
            .ok_or_else(|| "ckbus response has no result".into())
    }
}

async fn issue(route: &dyn CredentialRoute) -> Result<Credential, String> {
    serde_json::from_value(
        route
            .call("ckbus.credential", serde_json::json!({}))
            .await?,
    )
    .map_err(|_| "invalid ckbus credential response".into())
}
async fn renew(route: &dyn CredentialRoute, credential: &mut Credential) -> Result<(), String> {
    let value = route
        .call(
            "ckbus.credential_renew",
            serde_json::json!({"credential_public":credential.credential_public}),
        )
        .await?;
    if value["credential_public"].as_str() != Some(&credential.credential_public) {
        return Err("ckbus renewal changed credential public key".into());
    }
    let (Some(jwt), Some(exp)) = (value["jwt"].as_str(), value["exp"].as_u64()) else {
        return Err("invalid ckbus renewal response".into());
    };
    credential.jwt = jwt.into();
    credential.exp = exp;
    Ok(())
}
async fn nonce_signature(
    route: &dyn CredentialRoute,
    public: &str,
    nonce: &[u8],
) -> Result<Vec<u8>, String> {
    let result = route
        .call(
            "ckbus.nonce_sign",
            serde_json::json!({"credential_public":public,"nonce_b64":STANDARD.encode(nonce)}),
        )
        .await?;
    STANDARD
        .decode(
            result
                .get("signature_b64")
                .and_then(|v| v.as_str())
                .ok_or("ckbus nonce signature absent")?,
        )
        .map_err(|_| "invalid ckbus nonce signature".into())
}

pub struct NatsPublisher {
    pub stream: NatsStream,
    pub account: AccountNames,
}
#[async_trait]
impl BusPublisher for NatsPublisher {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        self.stream
            .publish(
                &event.subject,
                &event.event_key,
                event.digest(),
                event.headers.clone(),
            )
            .await
            .map_err(|_| "ckbus publish failed".into())
    }
}
impl NatsPublisher {
    async fn connect(
        credentials: Arc<dyn CredentialRoute>,
        credential: &Credential,
    ) -> Result<Self, String> {
        let config = ConnectConfig::new(&credential.credential_public)
            .map_err(|_| "invalid ckbus credential public key")?;
        if config.inbox_prefix() != credential.inbox_prefix {
            return Err("ckbus inbox prefix does not match credential".into());
        }
        let public = credential.credential_public.clone();
        let options = async_nats::ConnectOptions::with_jwt(credential.jwt.clone(), move |nonce| {
            let credentials = credentials.clone();
            let public = public.clone();
            // async-nats requires a Sync future; a JoinHandle wraps the Send-only subc RPC future.
            let signature = tokio::spawn(async move {
                nonce_signature(credentials.as_ref(), &public, &nonce).await
            });
            async move {
                signature
                    .await
                    .map_err(async_nats::AuthError::new)?
                    .map_err(async_nats::AuthError::new)
            }
        })
        .connection_timeout(Duration::from_secs(5));
        let account =
            AccountNames::derive(&credential.acct).map_err(|_| "invalid ckbus account")?;
        let conn = NatsConnection::connect(&credential.server_url, options, config)
            .await
            .map_err(|_| "ckbus connect failed")?;
        Ok(Self {
            stream: conn.stream(account.streams().event.clone()),
            account,
        })
    }
}

/// Account discovery is independent of connecting to NATS, so broker outages still queue events.
pub struct NatsConnector {
    route: Arc<dyn CredentialRoute>,
    current: Mutex<Option<Credential>>,
}
impl NatsConnector {
    pub fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            route: Arc::new(Credentials::new(conn)),
            current: Mutex::new(None),
        }
    }
}
#[async_trait]
impl BusConnector for NatsConnector {
    async fn connect(&self) -> Result<BusConnection, String> {
        let mut current = self.current.lock().await;
        let mut credential = match current.take() {
            Some(value) => value,
            None => issue(self.route.as_ref()).await?,
        };
        if now_ms() / 1000 + 300 >= credential.exp
            && renew(self.route.as_ref(), &mut credential).await.is_err()
        {
            credential = issue(self.route.as_ref()).await?;
        }
        let account =
            AccountNames::derive(&credential.acct).map_err(|_| "invalid ckbus account")?;
        let refresh_at_ms = credential.exp.saturating_mul(1000).saturating_sub(300_000);
        if refresh_at_ms <= now_ms() {
            return Err("ckbus credential expires before its refresh margin".into());
        }
        let publisher = Arc::new(LazyPublisher {
            route: self.route.clone(),
            account: account.clone(),
            state: Mutex::new(LazyState {
                credential: credential.clone(),
                publisher: None,
                reissue: false,
            }),
        });
        *current = Some(credential);
        Ok(BusConnection {
            publisher,
            account,
            refresh_at_ms,
        })
    }
}

struct LazyPublisher {
    route: Arc<dyn CredentialRoute>,
    account: AccountNames,
    state: Mutex<LazyState>,
}
struct LazyState {
    credential: Credential,
    publisher: Option<NatsPublisher>,
    reissue: bool,
}
#[async_trait]
impl BusPublisher for LazyPublisher {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        let mut state = self.state.lock().await;
        if state.publisher.is_none() {
            if state.reissue {
                let credential = issue(self.route.as_ref()).await?;
                if credential.acct != self.account.account() {
                    return Err("ckbus account changed during publish retry".into());
                }
                state.credential = credential;
            }
            state.reissue = true;
            state.publisher =
                Some(NatsPublisher::connect(self.route.clone(), &state.credential).await?);
        }
        let result = state.publisher.as_ref().unwrap().publish(event).await;
        if result.is_err() {
            state.publisher = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{collections::VecDeque, sync::Mutex as StdMutex};
    type ExpectedCall = (
        &'static str,
        serde_json::Value,
        Result<serde_json::Value, String>,
    );
    struct FakeRoute(StdMutex<VecDeque<ExpectedCall>>);
    #[async_trait]
    impl CredentialRoute for FakeRoute {
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            let (expected, body, result) = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected credential request");
            assert_eq!(method, expected);
            assert_eq!(params, body);
            result
        }
    }
    #[tokio::test]
    async fn bus_credentials_issue_sign_and_renew_documented_ckbus_shapes() {
        let issued = json!({"jwt":"fixture-initial-jwt","exp":1900000900u64,"acct":"box_test","account_public":"A_fixture","inbox_prefix":"_INBOX.U_fixture","server_url":"nats://127.0.0.1:4222","credential_public":"U_fixture","user_jwt_id":"j1","spawn_generation":"g1","credential_epoch":1});
        let renewed = json!({"jwt":"fixture-renewed-jwt","exp":1900001800u64,"user_jwt_id":"j2","credential_public":"U_fixture","spawn_generation":"g1","credential_epoch":1});
        let route = FakeRoute(StdMutex::new(VecDeque::from([
            ("ckbus.credential", json!({}), Ok(issued)),
            (
                "ckbus.nonce_sign",
                json!({"nonce_b64":STANDARD.encode(b"fixture nonce"),"credential_public":"U_fixture"}),
                Ok(json!({"signature_b64":STANDARD.encode([7u8;64])})),
            ),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(renewed),
            ),
        ])));
        let mut credential = issue(&route).await.unwrap();
        assert_eq!(credential.acct, "box_test");
        assert_eq!(
            nonce_signature(&route, &credential.credential_public, b"fixture nonce")
                .await
                .unwrap(),
            [7u8; 64]
        );
        renew(&route, &mut credential).await.unwrap();
        assert_eq!(credential.jwt, "fixture-renewed-jwt");
        assert_eq!(credential.exp, 1900001800);
        assert_eq!(credential.credential_public, "U_fixture");
        assert!(route.0.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn bus_credentials_refusal_and_bad_renewal_do_not_replace_live_credential() {
        let route = FakeRoute(StdMutex::new(VecDeque::from([
            ("ckbus.credential", json!({}), Err("ckbus_not_ready".into())),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(json!({"jwt":"bad","exp":42,"credential_public":"other"})),
            ),
            (
                "ckbus.nonce_sign",
                json!({"nonce_b64":STANDARD.encode(b"n"),"credential_public":"U_fixture"}),
                Ok(json!({"signature_b64":"!"})),
            ),
        ])));
        assert!(issue(&route).await.is_err());
        let mut credential = Credential {
            jwt: "original".into(),
            exp: 10,
            acct: "box_test".into(),
            inbox_prefix: "_INBOX.U_fixture".into(),
            server_url: "nats://127.0.0.1:4222".into(),
            credential_public: "U_fixture".into(),
        };
        assert!(renew(&route, &mut credential).await.is_err());
        assert_eq!(credential.jwt, "original");
        assert_eq!(credential.exp, 10);
        assert!(nonce_signature(&route, "U_fixture", b"n").await.is_err());
    }
    #[tokio::test]
    async fn bus_account_discovery_and_renewal_do_not_wait_for_broker_connection() {
        let exp = now_ms() / 1000 + 900;
        let route = Arc::new(FakeRoute(StdMutex::new(VecDeque::from([
            (
                "ckbus.credential",
                json!({}),
                Ok(
                    json!({"jwt":"fixture","exp":exp,"acct":"box_test","inbox_prefix":"_INBOX.U_fixture","server_url":"nats://127.0.0.1:1","credential_public":"U_fixture"}),
                ),
            ),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(json!({"jwt":"renewed","exp":exp,"credential_public":"U_fixture"})),
            ),
        ]))));
        let connector = NatsConnector {
            route: route.clone(),
            current: Mutex::new(None),
        };
        let connection = connector.connect().await.unwrap();
        assert_eq!(connection.account.account(), "box_test");
        connector.current.lock().await.as_mut().unwrap().exp = now_ms() / 1000 + 60;
        let renewed = connector.connect().await.unwrap();
        assert!(renewed.refresh_at_ms > now_ms());
        assert!(route.0.lock().unwrap().is_empty());
    }
}
