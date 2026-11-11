//! Optional routing groups, read through the admission transport and cached off the run path.

use std::{collections::BTreeMap, time::Duration};

use cortexkit_role_routing::describe::{check_describe, RoleDescribe};

use crate::account_routing::{RoutingClient, SubcRoutingTransport, ADMISSION_BUDGET};

/// Replace this local name with the contract crate's constant once broca pins a release
/// that defines the replay guarantee for admissions whose runs have moved accounts.
pub const ACCOUNT_CONTINUE_RESUME: &str = "account_continue_resume";

/// Whether routing explicitly declares an optional operation group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilitySupport {
    Declared,
    NotDeclared,
    /// No usable description was received. This is not a negative declaration.
    Unknown,
}

impl RoutingClient {
    /// Whether replaying an admission returns the account a moved run currently uses.
    /// Continuing alone does not promise that recovery can recover the latest selection.
    pub async fn answers_moved_runs_with_current_account(&self) -> bool {
        self.capability(cortexkit_role_routing::capabilities::ACCOUNT_CONTINUE)
            .await
            == CapabilitySupport::Declared
            && self.capability(ACCOUNT_CONTINUE_RESUME).await == CapabilitySupport::Declared
    }
}

// A missing provider has no route whose closure could invalidate the cache. The age bound
// lets discovery recover even in that case, and bounds missed registration notifications.
const MAX_AGE: Duration = Duration::from_secs(60);

struct Cached {
    route: Option<broca_subc::RouteHandle>,
    read_at: tokio::time::Instant,
    describe: Option<RoleDescribe>,
}

#[derive(Default)]
struct State {
    cached: Option<Cached>,
    logged: BTreeMap<String, CapabilitySupport>,
}

/// Shared by all runs using one admission transport. A closed route (including routing's
/// re-registration) drops its description; every answer also expires after one minute.
/// Unknown answers are cached too, so an unavailable router is not probed on every quota
/// exhaustion that might otherwise ask routing for another account.
#[derive(Default)]
pub(crate) struct RoutingCapabilities(tokio::sync::Mutex<State>);

impl RoutingCapabilities {
    pub(crate) async fn read(
        &self,
        transport: &SubcRoutingTransport,
        name: &str,
    ) -> CapabilitySupport {
        let mut state = self.0.lock().await;
        let fresh = state.cached.as_ref().is_some_and(|cached| {
            cached.read_at.elapsed() < MAX_AGE
                && cached
                    .route
                    .is_none_or(|route| transport.capability_route_is_open(route))
        });
        if !fresh {
            let reply =
                tokio::time::timeout(ADMISSION_BUDGET, transport.describe_for_capabilities()).await;
            let (route, describe) = match reply {
                Ok(Ok((route, reply))) => (Some(route), decode(reply)),
                _ => (None, None),
            };
            state.cached = Some(Cached {
                route,
                read_at: tokio::time::Instant::now(),
                describe,
            });
        }
        let answer = match state.cached.as_ref().and_then(|c| c.describe.as_ref()) {
            Some(describe) if describe.serves(name) => CapabilitySupport::Declared,
            Some(_) => CapabilitySupport::NotDeclared,
            None => CapabilitySupport::Unknown,
        };
        if state.logged.insert(name.to_owned(), answer) != Some(answer) {
            tracing::info!(target: "accounts", capability = name, ?answer, "routing capability changed");
        }
        answer
    }
}

fn decode(reply: Result<Vec<u8>, Vec<u8>>) -> Option<RoleDescribe> {
    #[derive(serde::Deserialize)]
    struct Response {
        result: RoleDescribe,
    }
    let describe = serde_json::from_slice::<Response>(&reply.ok()?)
        .ok()?
        .result;
    check_describe(&describe).ok()?;
    Some(describe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_routing::{AccountSelectionConfig, RoutingReply, RoutingTransport};
    use std::sync::Arc;

    struct Capabilities(CapabilitySupport, CapabilitySupport);

    #[async_trait::async_trait]
    impl RoutingTransport for Capabilities {
        async fn capability(&self, name: &str) -> CapabilitySupport {
            match name {
                cortexkit_role_routing::capabilities::ACCOUNT_CONTINUE => self.0,
                ACCOUNT_CONTINUE_RESUME => self.1,
                _ => panic!("unexpected capability {name}"),
            }
        }

        async fn request(&self, _body: Vec<u8>) -> RoutingReply {
            panic!("capability helper must use the transport's cached reader")
        }
    }

    #[tokio::test]
    async fn routing_capabilities_current_account_replay_requires_both_declarations() {
        use CapabilitySupport::*;
        for (continuation, resume, expected) in [
            (Declared, Declared, true),
            (Declared, NotDeclared, false),
            (Declared, Unknown, false),
            (NotDeclared, Declared, false),
            (NotDeclared, NotDeclared, false),
            (NotDeclared, Unknown, false),
            (Unknown, Declared, false),
            (Unknown, NotDeclared, false),
            (Unknown, Unknown, false),
        ] {
            let client = RoutingClient::new(
                Arc::new(Capabilities(continuation, resume)),
                AccountSelectionConfig::default(),
            );
            assert_eq!(
                client.answers_moved_runs_with_current_account().await,
                expected,
                "continue={continuation:?}, resume={resume:?}"
            );
        }
    }
}
