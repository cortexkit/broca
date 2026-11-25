//! Open a scoped extractor role on a module's management surface.

use std::collections::BTreeMap;

use super::{
    BindIdentity, ConnResult, GateTarget, RouteHandle, RouteOpenRetry, RouteTarget, SubcConnection,
};

impl SubcConnection {
    /// Open the extractor role under a daemon-stamped work scope.
    pub async fn route_open_extractor_scoped_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: subc_protocol::scope::ScopeSelector,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::management_surface(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ManagementSurface {
                    module_id: module_id.into(),
                },
                identity,
                Some(scope.clone()),
                Some(BTreeMap::from([("extractor".into(), "v1".into())])),
            )
        })
        .await
    }
}
