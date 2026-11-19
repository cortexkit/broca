//! Route facts on the bus (routing 0.7.36 §8.1). Each account observation broca reports to
//! routing is also published as a `route_fact.v1` event, `{op, params}`, under the fact's own
//! `event_id` as the bus message id. While routing declares `route_fact_stream` (its bus reader
//! is bound and covers every route fact), an acknowledged publish is the whole delivery. While it
//! doesn't, or its description can't be read, the fact also goes out as the direct call, as it
//! always has. A publish the bus doesn't acknowledge also falls back to the direct call, so a
//! bus that is down never loses a fact. Routing applies a fact that arrives both ways once.

use async_trait::async_trait;
use broca_store::bus_events::{BusPublisher, Publication};
use serde_json::{json, Value};

/// The event name and version of a route fact.
pub const ROUTE_FACT_EVENT: &str = "route_fact";
pub const ROUTE_FACT_VERSION: u32 = 1;
/// The largest event body the bus carries. A larger fact goes only as the direct call.
pub const MAX_ROUTE_FACT_BYTES: usize = 4096;
/// The bus header carrying the body's digest, as `cortexkit-bus-client`'s `publish_event`
/// writes it.
const CONTENT_DIGEST_HEADER: &str = "Ck-Content-Digest";

/// Publishes route facts on the bus.
#[async_trait]
pub trait RouteFactPublisher: Send + Sync {
    /// Publish `{op, params}` under the bus message id `fact_id`. `Ok` only once the stream has
    /// acknowledged it.
    async fn publish_route_fact(
        &self,
        op: &str,
        params: &Value,
        fact_id: &str,
    ) -> Result<(), String>;
}

/// A route fact's event body, refused over [`MAX_ROUTE_FACT_BYTES`].
pub fn fact_body(op: &str, params: &Value) -> Result<Vec<u8>, String> {
    let body = serde_json::to_vec(&json!({"op": op, "params": params}))
        .map_err(|error| error.to_string())?;
    if body.len() > MAX_ROUTE_FACT_BYTES {
        return Err(format!(
            "a route fact of {} bytes is over the {MAX_ROUTE_FACT_BYTES}-byte event limit",
            body.len()
        ));
    }
    Ok(body)
}

/// The publication of one route fact on `account`'s event stream: broca's `route_fact.v1`
/// subject, the fact id as message id, and the body inline with its digest header.
pub fn publication(
    account: &cortexkit_bus_naming::AccountNames,
    op: &str,
    params: &Value,
    fact_id: &str,
) -> Result<Publication, String> {
    let body = fact_body(op, params)?;
    let subject = account
        .event_subject("broca", ROUTE_FACT_EVENT, ROUTE_FACT_VERSION)
        .map_err(|error| error.to_string())?;
    let mut publication = Publication {
        event_key: fact_id.to_owned(),
        subject,
        headers: Default::default(),
        body,
        digest: None,
        inline: true,
    };
    publication.headers.insert(
        CONTENT_DIGEST_HEADER.into(),
        publication.digest().to_string(),
    );
    Ok(publication)
}

/// Publish one route fact through `publisher`, the account's shared bus link.
pub async fn publish(
    publisher: &dyn BusPublisher,
    account: &cortexkit_bus_naming::AccountNames,
    op: &str,
    params: &Value,
    fact_id: &str,
) -> Result<(), String> {
    let publication = publication(account, op, params, fact_id)?;
    publisher.publish(&publication).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fact_is_its_op_and_params_on_broca_route_fact_subject() {
        let account = cortexkit_bus_naming::AccountNames::derive("box_test").unwrap();
        let params = json!({"event_id": "adm1:run_finished", "kind": "run_finished"});
        let publication = publication(
            &account,
            "route.account_observe",
            &params,
            "adm1:run_finished",
        )
        .unwrap();
        assert_eq!(publication.subject, "ck.box_test.event.broca.route_fact.v1");
        assert_eq!(publication.event_key, "adm1:run_finished");
        assert!(publication.inline);
        let body: Value = serde_json::from_slice(&publication.body).unwrap();
        assert_eq!(
            body,
            json!({"op": "route.account_observe", "params": params})
        );
        assert_eq!(
            publication.headers.get(CONTENT_DIGEST_HEADER),
            Some(&publication.digest().to_string())
        );
    }

    use crate::account_routing::{
        AccountSelectionConfig, RoutingClient, RoutingReply, RoutingTransport, ACCOUNT_OBSERVE_OP,
    };
    use crate::routing_capabilities::CapabilitySupport;
    use std::sync::{Arc, Mutex};

    /// Routing that answers every request and describes `route_fact_stream` as `stream`.
    struct Routing {
        stream: CapabilitySupport,
        requests: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl RoutingTransport for Routing {
        async fn capability(&self, name: &str) -> CapabilitySupport {
            match name {
                cortexkit_role_routing::capabilities::ROUTE_FACT_STREAM => self.stream,
                _ => CapabilitySupport::NotDeclared,
            }
        }
        async fn request(&self, body: Vec<u8>) -> RoutingReply {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            Ok(Ok(br#"{"result":{}}"#.to_vec()))
        }
    }

    /// A bus that acknowledges each publish when `acks`, recording what it was given.
    struct Bus {
        acks: bool,
        published: Mutex<Vec<(String, Value, String)>>,
    }

    #[async_trait]
    impl RouteFactPublisher for Bus {
        async fn publish_route_fact(
            &self,
            op: &str,
            params: &Value,
            fact_id: &str,
        ) -> Result<(), String> {
            self.published
                .lock()
                .unwrap()
                .push((op.into(), params.clone(), fact_id.into()));
            if self.acks {
                Ok(())
            } else {
                Err("no acknowledgement".into())
            }
        }
    }

    /// Reports one observation with routing describing `stream` and a bus that `acks` (none
    /// when `None`), and returns the direct calls and the publishes it made.
    async fn observe(
        stream: CapabilitySupport,
        acks: Option<bool>,
    ) -> (Vec<Value>, Vec<(String, Value, String)>) {
        let routing = Arc::new(Routing {
            stream,
            requests: Mutex::default(),
        });
        let client = RoutingClient::new(routing.clone(), AccountSelectionConfig::default());
        let bus = acks.map(|acks| {
            Arc::new(Bus {
                acks,
                published: Mutex::default(),
            })
        });
        if let Some(bus) = &bus {
            client.set_route_facts(bus.clone());
        }
        client.observe(observation()).await;
        let requests = routing.requests.lock().unwrap().clone();
        let published = bus.map_or_else(Vec::new, |bus| bus.published.lock().unwrap().clone());
        (requests, published)
    }

    fn observation() -> Value {
        json!({"event_id": "adm1:run_finished", "kind": "run_finished", "admission_id": "adm1"})
    }

    fn the_fact() -> (String, Value, String) {
        (
            ACCOUNT_OBSERVE_OP.into(),
            observation(),
            "adm1:run_finished".into(),
        )
    }

    fn the_call() -> Value {
        json!({"method": ACCOUNT_OBSERVE_OP, "params": observation()})
    }

    #[tokio::test]
    async fn while_routing_reads_route_facts_an_acknowledged_publish_is_the_whole_delivery() {
        let (requests, published) = observe(CapabilitySupport::Declared, Some(true)).await;
        assert_eq!(published, [the_fact()]);
        assert!(requests.is_empty(), "no direct call: {requests:?}");
    }

    #[tokio::test]
    async fn a_publish_the_bus_does_not_acknowledge_also_goes_as_the_direct_call() {
        let (requests, published) = observe(CapabilitySupport::Declared, Some(false)).await;
        assert_eq!(published, [the_fact()]);
        assert_eq!(requests, [the_call()]);
    }

    #[tokio::test]
    async fn while_routing_reads_no_route_facts_an_observation_goes_both_ways() {
        // An unread description is not a declaration.
        for stream in [CapabilitySupport::NotDeclared, CapabilitySupport::Unknown] {
            let (requests, published) = observe(stream, Some(true)).await;
            assert_eq!(published, [the_fact()], "{stream:?}");
            assert_eq!(requests, [the_call()], "{stream:?}");
        }
    }

    #[tokio::test]
    async fn without_a_bus_link_an_observation_goes_only_as_the_direct_call() {
        let (requests, _) = observe(CapabilitySupport::Declared, None).await;
        assert_eq!(requests, [the_call()]);
    }

    /// With `BROCA_ROUTE_FACT_SAMPLES` set to a folder, writes the publications of an
    /// admission start and a run finish, built from the real observation bodies, for routing to
    /// check its reader against.
    #[test]
    fn route_fact_samples() {
        let Some(folder) = std::env::var_os("BROCA_ROUTE_FACT_SAMPLES") else {
            return;
        };
        let folder = std::path::PathBuf::from(folder);
        std::fs::create_dir_all(&folder).unwrap();
        let account = cortexkit_bus_naming::AccountNames::derive("box_test").unwrap();
        let headers = std::collections::BTreeMap::from([(
            "x-ratelimit-remaining-requests".to_string(),
            "41".to_string(),
        )]);
        for (name, params) in [
            (
                "admission-started",
                crate::account_routing::admission_started_body(
                    "adm1:admission_started",
                    "adm1",
                    "run-1",
                ),
            ),
            (
                "run-finished",
                crate::account_routing::run_finished_body(
                    "adm1:run_finished",
                    "adm1",
                    "run-1",
                    broca_protocol::RunFinishReason::Completed,
                    Some(&headers),
                ),
            ),
        ] {
            let fact_id = params["event_id"].as_str().unwrap().to_owned();
            let publication = publication(&account, ACCOUNT_OBSERVE_OP, &params, &fact_id).unwrap();
            let sample = json!({
                "subject": publication.subject,
                "nats_msg_id": publication.event_key,
                "headers": publication.headers,
                "body": String::from_utf8(publication.body.clone()).unwrap(),
            });
            std::fs::write(
                folder.join(format!("{name}.json")),
                serde_json::to_vec_pretty(&sample).unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    fn a_fact_over_the_event_limit_is_refused() {
        let params = json!({"event_id": "e", "note": "x".repeat(MAX_ROUTE_FACT_BYTES)});
        assert!(fact_body("route.account_observe", &params).is_err());
    }
}
