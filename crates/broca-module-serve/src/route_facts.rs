//! Route facts on the bus (routing 0.7.36 §8.1). Each account observation broca reports to
//! routing is also published as a `route_fact.v1` event, `{op, params}`, under the fact's own
//! `event_id` as the bus message id. While routing declares `route_fact_stream` (its bus reader
//! is bound and covers every route fact), an acknowledged publish is the whole delivery. While it
//! doesn't, or its description can't be read, the fact also goes out as the direct call, as it
//! always has. A publish the bus doesn't acknowledge also falls back to the direct call, so a
//! bus that is down never loses a fact. Routing applies a fact that arrives both ways once.

use async_trait::async_trait;
use broca_store::bus_events::BusPublisher;
pub use broca_store::bus_events::{
    route_fact_body as fact_body, MAX_ROUTE_FACT_BYTES, ROUTE_FACT_EVENT, ROUTE_FACT_VERSION,
};
use serde_json::Value;

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

/// Where routing reports that a run's end stored (see `broca_module::store_run_events`) are
/// settled once the run's own report reaches routing, so the stored copy is not sent again.
pub trait RouteFactOutbox: Send + Sync {
    fn settle(&self, fact_id: &str);
}

/// The publication of one route fact on `account`'s event stream: broca's `route_fact.v1`
/// subject, the fact id as message id, and the body inline with its digest header.
pub fn publication(
    account: &cortexkit_bus_naming::AccountNames,
    op: &str,
    params: &Value,
    fact_id: &str,
) -> Result<broca_store::bus_events::Publication, String> {
    broca_store::bus_events::route_fact_publication(account, fact_id, fact_body(op, params)?)
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
    use serde_json::json;

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
            publication.headers.get("Ck-Content-Digest"),
            Some(&publication.digest().to_string())
        );
    }

    use crate::account_routing::{
        AccountSelectionConfig, RoutingClient, RoutingReply, RoutingTransport, ACCOUNT_OBSERVE_OP,
    };
    use crate::routing_capabilities::CapabilitySupport;
    use std::sync::{Arc, Mutex};

    /// Routing that describes `route_fact_stream` as `stream` and answers every request, or
    /// none when `away` (the call fails as a daemon cut makes it fail).
    struct Routing {
        stream: CapabilitySupport,
        requests: Mutex<Vec<Value>>,
        away: bool,
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
            if self.away {
                return Err("routing is away".into());
            }
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
        let (requests, published, _) = observe_settling(stream, acks, false).await;
        (requests, published)
    }

    /// [`observe`] with routing `away` or not, also returning the stored facts it settled.
    async fn observe_settling(
        stream: CapabilitySupport,
        acks: Option<bool>,
        away: bool,
    ) -> (Vec<Value>, Vec<(String, Value, String)>, Vec<String>) {
        let routing = Arc::new(Routing {
            stream,
            requests: Mutex::default(),
            away,
        });
        let outbox = Arc::new(Settled::default());
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
        client.set_route_fact_outbox(outbox.clone());
        client.observe(observation()).await;
        let requests = routing.requests.lock().unwrap().clone();
        let published = bus.map_or_else(Vec::new, |bus| bus.published.lock().unwrap().clone());
        let settled = outbox.0.lock().unwrap().clone();
        (requests, published, settled)
    }

    /// The stored facts settled, in order.
    #[derive(Default)]
    struct Settled(Mutex<Vec<String>>);

    impl RouteFactOutbox for Settled {
        fn settle(&self, fact_id: &str) {
            self.0.lock().unwrap().push(fact_id.into());
        }
    }

    /// A run's end report settles its stored copy once routing has it, by an acknowledged
    /// publish or an answered call. When neither reached routing (it and the bus were away, as
    /// at a daemon cut), the stored copy stays, for the outbox to deliver.
    #[tokio::test]
    async fn a_delivered_end_report_settles_its_stored_copy_and_an_undelivered_one_keeps_it() {
        let settled = |requests_reached: bool| {
            if requests_reached {
                vec!["adm1:run_finished".to_string()]
            } else {
                Vec::new()
            }
        };
        let (_, _, by_publish) =
            observe_settling(CapabilitySupport::Declared, Some(true), true).await;
        assert_eq!(
            by_publish,
            settled(true),
            "an acknowledged publish delivers it"
        );
        let (_, _, by_call) = observe_settling(CapabilitySupport::NotDeclared, None, false).await;
        assert_eq!(by_call, settled(true), "an answered call delivers it");
        let (requests, published, kept) =
            observe_settling(CapabilitySupport::Declared, Some(false), true).await;
        assert_eq!((requests.len(), published.len()), (1, 1));
        assert_eq!(kept, settled(false), "neither reached routing");
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
